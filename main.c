#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h> // offsetof()
#include <errno.h> // errno, ENOENT
#include <fcntl.h> // open(), O_WRONLY, O_CREAT, O_APPEND
#include <unistd.h> // read(), write(), fsync(), close()

#define WAL_PATH     "wal.log"
#define MAX_ACCOUNTS 100

// Ordered largest-to-smallest (8, 8, 4, 4 bytes = 24 bytes exact, zero padding)
typedef struct {
    uint64_t lsn; //Log sequence number (monotonically increasing ID)
    int64_t delta; // Amount in cents (+1000 for deposit, -500 for debit)
    uint32_t account_id; // Target account (e.g., 0 to 99)
    uint32_t checksum; // Used to detect torn writes
} WalRecord;

// FNV-1a 32-bit hash over the first 20 bytes (everything before .checksum)
uint32_t compute_checksum(const WalRecord *rec) {
    const uint8_t *bytes = (const uint8_t *)rec;
    size_t len = offsetof(WalRecord, checksum); // 20 bytes
}

// Appends a single record to the WAL and forces it to physical storage.
// Returns 0 on sucess, -1 on failure.
int wal_append(int fd, const WalRecord *rec) {
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
    int fd = open(path, O_RDONLY);
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

    while ((bytes_read = read(fd, &rec, sizeof(WalRecord))) > 0) {
        // Did we hit a partial/tuncated record at the end of the file?
        if (bytes_read != (ssize_t)sizeof(WalRecord)) {
            fprintf(stderr, "WARNING: Partial reord detected (%zd bytes). Stopping replay.\n", bytes_read);
            break;
        }

        if (rec.account_id < MAX_ACCOUNTS) {
            balances[rec.account_id] += rec.delta;
        }
        *last_lsn = rec.lsn;
    }

    if (bytes_read == -1) {
        perror("read failed during recovery.");
        close(fd);
        return -1;
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

    // Always recover state from disk first
    if (wal_recover(WAL_PATH, balances, &last_lsn) != 0) {
        return EXIT_FAILURE;
    }

    // If user passed <account_id> <delta>, append a new transaction
    if (argc == 3) {
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