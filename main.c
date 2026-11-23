#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>     // offsetof()
#include <string.h>     // strcmp(), memcpy()
#include <errno.h>      // errno, ENOENT
#include <fcntl.h>      // open(), O_WRONLY, O_CREAT, O_APPEND
#include <unistd.h>     // read(), write(), fsync(), close()

#define WAL_PATH        "wal.log"
#define SNAP_PATH       "snapshot.dat"
#define SNAP_TMP_PATH   "snapshot.tmp"
#define MAX_ACCOUNTS    100

// Ordered largest-to-smallest (8, 8, 4, 4 bytes = 24 bytes exact, zero padding)
typedef struct {
    uint64_t lsn; //Log sequence number (monotonically increasing ID)
    int64_t delta; // Amount in cents (+1000 for deposit, -500 for debit)
    uint32_t account_id; // Target account (e.g., 0 to 99)
    uint32_t checksum; // Used to detect torn writes
} WalRecord;

// Fixed-size binary snapshot of the entire ledger state
typedef struct {
    uint64_t last_lsn;
    int64_t balances[MAX_ACCOUNTS];
} Snapshot;

// FNV-1a 32-bit hash over the first 20 bytes (everything before .checksum)
uint32_t compute_checksum(const WalRecord *rec) {
    const uint8_t *bytes = (const uint8_t *)rec;
    size_t len = offsetof(WalRecord, checksum); // 20 bytes
    uint32_t hash = 2166136261u;                // FNV offset basis

    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= 16777619u;                      // FNV prime
    }
    return hash;
}

// Load the latest checkpoint snapshot into memory (if one exists)
int snapshot_load(const char *path, int64_t *balances, uint64_t *last_lsn) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno == ENOENT) {
            return 0; // No checkpoint yet
        }
        perror("open snapshot failed.");
        return -1;
    }

    Snapshot snap;
    ssize_t bytes_read = read(fd, &snap, sizeof(Snapshot));
    close(fd);

    if (bytes_read != (ssize_t)sizeof(Snapshot)) {
        fprintf(stderr, "ERROR: Corrupted snapshot file.\n");
        return -1;
    }

    memcpy(balances, snap.balances, sizeof(snap.balances));
    *last_lsn = snap.last_lsn;
    printf("Loaded checkpoint snapshot (LSN: $%lu)\n", *last_lsn);
    return 0;
}

// Atomically write snapshot.dat, then truncate wal.log
int checkpoint_create(const int64_t *balances, uint64_t last_lsn) {
    Snapshot snap;
    snap.last_lsn = last_lsn;
    memcpy(snap.balances, balances, sizeof(snap.balances));

    // Write to a temporary file first
    int fd = open(SNAP_TMP_PATH, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("open snapshot.tmp failed");
        return -1;
    }

    if (write(fd, &snap, sizeof(Snapshot)) != (ssize_t)sizeof(Snapshot)) {
        perror("write snapshot.tmp failed");
        close(fd);
        return -1;
    }

    // Force temp file bytes to physical disk
    if (fsync(fd) == -1) {
        perror("fsync snapshot.tmp failed.");
        close(fd);
        return -1;
    }
    close(fd);

    // Atomic swap: replaces snapshot.dat in a single kernel operation
    if (rename(SNAP_TMP_PATH, SNAP_PATH) == -1) {
        perror("Rename snapshot failed.");
        return -1;
    }

    // fsync the parent directory so the rename metadata itself is durable
    int dir_fd = open(".", O_RDONLY);
    if (dir_fd != -1) {
        fsync(dir_fd);
        close(dir_fd);
    }

    // Safe to truncate the WAL now that the snapshot is durable
    int wal_fd = open(WAL_PATH, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (wal_fd == -1) {
        perror("Open wal fo truncate failed.");
        return -1;
    }
    fsync(wal_fd);
    close(wal_fd);

    printf("CHECKPOINT COMPLETE: Save up to LSN %lu and truncated %s.\n", last_lsn, WAL_PATH);
    return 0;
}

// Appends a single record to the WAL and forces it to physical storage.
// Returns 0 on sucess, -1 on failure.
int wal_append(int fd, WalRecord *rec) {
    // Stamp the checksum right before writing to disk
    rec->checksum = compute_checksum(rec);

    ssize_t bytes_written = write(fd, rec, sizeof(WalRecord));
    if (bytes_written != (ssize_t)sizeof(WalRecord)) {
        perror("write failed");
        return -1;
    }

    // The Durability Barrier:
    // Blocks until the OS flushes the kernel page cache to the physical disk
    if (fsync(fd) == -1) {
        perror("fsync failed.");
        return -1;
    }

    return 0;
}

// Replays wal.log from byte 0 to rebuild in-memory balances and find last_lsn.
int wal_recover(const char *path, int64_t *balances, uint64_t *last_lsn) {
    // Open O_RDWR so we can ftruncate() if we find corruption
    int fd = open(path, O_RDWR);
    if (fd == -1) {
        // If wal.log doesn't exist yet, that's a fresh DB, not an error
        if (errno == ENOENT) {
            return 0;
        }
        perror("Open for recovery failed.");
        return -1;
    } 

    WalRecord rec;
    ssize_t bytes_read;
    off_t valid_offset = 0; // Tracks the exact byte offset of the last good record
    int needs_truncate = 0;
    int replayed = 0;

    while ((bytes_read = read(fd, &rec, sizeof(WalRecord))) > 0) {
        // Did we hit a partial/tuncated record at the end of the file?
        if (bytes_read != (ssize_t)sizeof(WalRecord)) {
            fprintf(stderr, "WARNING: Partial record at offset %ld (%zd bytes). Discarding.\n",
                (long)valid_offset, bytes_read);
            needs_truncate = 1;
            break;
        }

        // Verify integrity before applying to in-memory state
        uint32_t expected = compute_checksum(&rec);
        if (rec.checksum != expected) {
            fprintf(
                stderr, 
                "WARNING: Checksum mismatch at offset %ld (LSN %lu). Expected 0x%08x. Discarding.\n",
                (long)valid_offset, rec.lsn, expected
            );
            needs_truncate = 1;
            break;
        }

        // Idempotency guard: only replay records newer than our snapshot
        if (rec.lsn > *last_lsn) {
            if (rec.account_id < MAX_ACCOUNTS) {
                balances[rec.account_id] += rec.delta;
            }
            *last_lsn = rec.lsn;
            replayed++;
        }

        valid_offset += sizeof(WalRecord);
    }

    if (bytes_read == -1) {
        perror("read failed during recovery.");
        close(fd);
        return -1;
    }

    // Heal the log by chopping off torn/corrupted tail bytes
    if (needs_truncate) {
        if (ftruncate(fd, valid_offset) == -1 || fsync(fd) == -1) {
            perror("Failed to trunate corrupted tail.");
            close(fd);
            return -1;
        }
        fprintf(stderr, "HEALED: Truncate %s back to %ld valid bytes.\n", path, (long)valid_offset);
    }

    if (replayed > 0) {
        printf("Replayed %d WAL record(s) (now at LSN: %lu)\n", replayed, *last_lsn);
    }

    close(fd);
    return 0;
}

void print_state(const int64_t *balances, uint64_t last_lsn) {
    printf("--- Recovered Ledger State (Last LSN: %lu) ---\n", last_lsn);
    for (int i = 0; i < MAX_ACCOUNTS; i++) {
        if (balances[i] != 0) {
            printf("Account %d: %ld cents ($%.2f)\n", i, balances[i], balances[i] / 100.00);
        }
    }
}

int main(int argc, char *argv[]) {
    // Zero-initialized in-memory state (simulates wiped RAM after a crash)
    int64_t balances[MAX_ACCOUNTS] = {0};
    uint64_t last_lsn = 0;

    // Load checkpoint snapshot first, then replay any newer WAL records
    if (snapshot_load(SNAP_PATH, balances, &last_lsn) != 0) {
        return EXIT_FAILURE;
    }

    // Always recover state from disk
    if (wal_recover(WAL_PATH, balances, &last_lsn) != 0) {
        return EXIT_FAILURE;
    }

    // Handle CLI commands
    if (argc == 2 && strcmp(argv[1], "checkpoint") == 0) {
        if (checkpoint_create(balances, last_lsn) != 0) {
            return EXIT_FAILURE;
        }
    } else if (argc == 3) {
        uint32_t acct = (uint32_t)strtoul(argv[1], NULL, 10);
        int64_t delta = strtoll(argv[2], NULL, 10);

        if (acct >= MAX_ACCOUNTS) {
            fprintf(stderr, "Account ID must be less than %d\n", MAX_ACCOUNTS);
            return EXIT_FAILURE;
        }

        // Open wal.log: Write only, create if missing, always append to the end.
        // 0644 = rw-r--r-- (owner read/write, group/others read)
        int fd = open(WAL_PATH, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd == -1) {
            perror("open for append failed");
            return EXIT_FAILURE;
        }

        WalRecord rec = {
            .lsn = last_lsn + 1,
            .delta = delta,
            .account_id = acct,
            .checksum = 0
        };

        // Write-AHEAD: Persist to WAL BEFORE updating in-memory balances
        if (wal_append(fd, &rec) != 0) {
            close(fd);
            return EXIT_FAILURE;
        }
        close(fd);
    
        // Safe to update memory now that disk has the record
        balances[acct] += delta;
        last_lsn = rec.lsn;
        printf("Committed LSN %lu (Account %u, Delta %ld)\n", rec.lsn, rec.account_id, rec.delta);
    } 

    // 3. Print current state
    print_state(balances, last_lsn);
    return EXIT_SUCCESS;
}