# Lock Manager

Rigorous two phase locking, Database course project, Baylor University.

## How to run

```shell
$ mvn clean package
$ java -jar target\\LockManager-1.0.jar input.txt
```

## Sample input

```
0
Start 0
Start 1
Read 0 0
Read 1 1
Write 1 0
Write 31 0
Write 0 1
Commit 0
EOF
```

## Sample output

```
Initial data: 00000000000000000000000000000000, Int value: 0

INPUT: Start 0
Start 0 ==> Transaction 0 has started

INPUT: Start 1
Start 1 ==> Transaction 1 has started

INPUT: Read 0 0
Read 0 0 ==> granted, value: 0

INPUT: Read 1 1
Read 1 1 ==> granted, value: 0

INPUT: Write 1 0
Write 1 0 ==> blocked

INPUT: Write 31 0
Write 31 0 ==> Transaction 0 is blocked, query added to pending list

INPUT: Write 0 1
Write 0 1 ==> blocked
Deadlock detected, rolling back transaction 1
ForceRollback 1 ==> Transaction 1 has been rolled back
Transaction 0 has been unblocked
Write 1 0 ==> granted, new value: 1
Write 31 0 ==> granted, new value: 1

INPUT: Commit 0
Write 1 0 ==> committed, new value: 1
Write 31 0 ==> committed, new value: 1
Commit 0 ==> Transaction 0 has been committed

Final results:
Dirty data: 10000000000000000000000000000010, Int value: 2147483650
Committed data: 10000000000000000000000000000010, Int value: 2147483650

Lock Table: (Mode: R = Read/Shared, W = Write/Exclusive) (Skipped Empty)

Transactions: (Mode: R = Read/Shared, W = Write/Exclusive)
TrID: 0, Status: COMMITTED, Holding (DataID/Mode): [], Waiting (DataID/Mode): [], Dependencies (TrID): []
TrID: 1, Status: ROLLBACK, Holding (DataID/Mode): [], Waiting (DataID/Mode): [], Dependencies (TrID): []
```

## Note

- First line of the input will contain an integer representing the initial value of the data.
- We maintain two copy of the data, dirty and committed. The committed data is only modified when a transaction is committed.
- If an operation of a transaction is blocked (waiting of locks), all upcoming operations of that transaction will not request any locks. We will put them in a pending list and process them once the transaction is unblocked.
- If a data item is in shared lock and there is a waiting exclusive lock request, we will put any upcoming shared lock request in the waiting list although the shared locked can be granted instantly. This is done to avoid starvation.
