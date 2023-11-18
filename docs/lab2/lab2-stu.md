# Lab2

## Part 3: All-or-nothing Atomicity

The redo-log protocol:

- A redo-log entry is started with a 32-bit transaction id (non-zero), and multiple operations.
- An operation consists of 64-bit block id (non-zero) and 4,096-byte data, which means the operation modifies the block to the data.
<!-- - The entry is ended with a 64-bit 0 and a 64-bit checksum, which indicates that the transaction is committed. The checksum is calculated with the transaction id and all operations. -->
- The entry is ended with a 64-bit 0, which indicates that the transaction is committed.
- After multiple redo-log entries, a 32-bit 0 is appended to denote the end of redo-log.

Redo-log size per entry: 20 + 4104 * operation_cnt (bytes)

Parsing:

1. Begin at the block id `KDefaultBlockCnt - KLogBlockCnt`, the offset 0.
2. Read a 32-bit integer. If it's 0, all log entries are read so far.
3. Else, it's the transaction id. Begin to record a log entry.
<!-- 4. Read a 64-bit block id. If it's 0, read the next 64-bit as the checksum, and verify it. If verified OK,  the log entry is terminated, and the transaction is considered to be committed. Then go to 2. If verified wrong, the redo-log is damaged, and we know that only the previous transactions are well-logged. -->
4. Read a 64-bit block id. If it's 0, the log entry is terminated, and the transaction is considered to be committed. Then go to 2.
5. Else, it's the block id. Begin to record an operation.
6. Read the following 4,096 bits, which are the data that need to be modified to. Go to 4.
