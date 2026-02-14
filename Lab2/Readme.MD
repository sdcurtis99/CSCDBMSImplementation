Concurrency Control Simulator Design Summary

This simulator executes multi-transaction schedules under two concurrency control schemes: Strict Two-Phase Locking (2PL) and MVCC with Snapshot Isolation (SI).

Under Strict 2PL, the engine maintains a lock table with S/X locks, FIFO wait queues, and a wait-for graph for deadlock detection. Locks are granted based on compatibility rules, upgrades from S to X are handled, and all locks are released only at COMMIT or ABORT. Deadlocks are detected via cycle detection in the wait-for graph, and the highest transaction ID in the cycle is deterministically chosen as the victim. Blocked operations are deferred and resumed in order once unblocked.

Under MVCC with Snapshot Isolation, each transaction receives a start timestamp at BEGIN and reads from a consistent snapshot. Writes are buffered privately and validated at COMMIT using first-committer-wins (write-write conflict detection). If a newer committed version exists for a written item, the transaction aborts; otherwise, new versions are installed with commit timestamps. No blocking occurs in MVCC mode.

The schedule runner processes events in file order, defers operations for blocked transactions, and resumes them in order upon unblocking, ensuring correct interleaving semantics. Extensive regression testing was performed to validate deadlock handling, upgrade behavior, snapshot stability, and write-write conflict detection.

To Run:

mode options --cc 2pl or mvcc

Required Fields
    Each event must contain:
        "t" — Transaction ID (integer)
        "op" — Operation type (string)
    Supported Operations
        "op" must be one of:
        "BEGIN"
        "R" (Read)
        "W" (Write)
        "COMMIT"
        "ABORT"

input scheulde using jsonl
ex:
{"t":1,"op":"BEGIN"}
{"t":2,"op":"BEGIN"}
{"t":1,"op":"R","item":"A"}
{"t":2,"op":"W","item":"B","value":200}
{"t":1,"op":"W","item":"A","value":150}
{"t":2,"op":"COMMIT"}
{"t":1,"op":"COMMIT"}

python sim.py --cc 2pl --schedule <schedule_file> --out <output_directory>
python sim.py --cc mvcc --schedule <schedule_file> --out <output_directory>