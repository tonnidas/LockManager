package com.baylor.lock;

import java.util.HashSet;
import java.util.Set;

import static com.baylor.lock.Storage.*;

public class Operations {
    public static void start(Query query) {
        if (transactions.containsKey(query.tID)) {
            query.status = Query.Status.SKIPPED;
            query.writeLog("Transaction " + query.tID + " already started before");
        } else {
            transactions.put(query.tID, Storage.TransactionStatus.ACTIVE);
            query.status = Query.Status.DONE;
            query.writeLog("Transaction " + query.tID + " has started");
        }
    }

    public static boolean verifyTransaction(Query query) {
        TransactionStatus status = transactions.getOrDefault(query.tID, TransactionStatus.NONE);

        if (status == TransactionStatus.NONE) {
            query.status = Query.Status.SKIPPED;
            query.writeLog("Transaction " + query.tID + " has not started");
            return false;
        } else if (status == TransactionStatus.COMMIT || status == TransactionStatus.ROLLBACK) {
            query.status = Query.Status.SKIPPED;
            query.writeLog("Transaction " + query.tID + " has already committed or rolled back");
            return false;
        } else if (status == TransactionStatus.BLOCK && query.command != Query.Command.ROLLBACK && query.command != Query.Command.FORCE_ROLLBACK) {
            query.status = Query.Status.PENDING;
            query.writeLog("Transaction " + query.tID + " is blocked, query added to pending list");
            return false;
        }

        return true;
    }

    public static void read(Query query) {
        if (!verifyTransaction(query)) {
            return;
        }

        boolean hasGranted = lockTable[query.dID].requestReadLock(query.tID);

        if (hasGranted) {
            int value = Storage.readData(query.dID);
            query.status = Query.Status.DONE;
            query.writeLog("granted, value: " + value);
        } else {
            transactions.put(query.tID, TransactionStatus.BLOCK);
            query.status = Query.Status.BLOCKED;
            query.writeLog("blocked");
        }
    }

    public static void write(Query query) {
        if (!verifyTransaction(query)) {
            return;
        }

        boolean hasGranted = lockTable[query.dID].requestWriteLock(query.tID);

        if (hasGranted) {
            int newValue = Storage.writeData(query.dID);
            query.status = Query.Status.DONE;
            query.writeLog("granted, new value: " + newValue);
        } else {
            transactions.put(query.tID, TransactionStatus.BLOCK);
            query.status = Query.Status.BLOCKED;
            query.writeLog("blocked");
        }
    }

    public static void commit(Query query) {
        if (!verifyTransaction(query)) {
            return;
        }

        releaseTransaction(query.tID);

        transactions.put(query.tID, TransactionStatus.COMMIT);
        query.status = Query.Status.DONE;
        query.writeLog("Transaction " + query.tID + " has been committed");
    }

    public static void rollback(Query query) {
        if (!verifyTransaction(query)) {
            return;
        }

        // undo all WRITE history in reverse order that were DONE
        for (int i = queries.length - 1; i >= 0; i--) {
            if (queries[i].tID == query.tID && queries[i].command == Query.Command.WRITE && queries[i].status == Query.Status.DONE) {

                int newValue = Storage.writeData(queries[i].dID);
                queries[i].status = Query.Status.ROLLBACK;
                queries[i].writeLog("rolled back, new value: " + newValue);
            }
        }

        releaseTransaction(query.tID);

        transactions.put(query.tID, TransactionStatus.ROLLBACK);
        query.status = Query.Status.DONE;
        query.writeLog("Transaction " + query.tID + " has been rolled back");
    }

    // release all locks and remove all dependencies
    public static void releaseTransaction(int tID) {
        for (int i = 0; i < 32; i++) {
            lockTable[i].unlock(tID);
        }
        Dependency.removeDependency(tID);
    }

    public static void organize() {
        // rollback until deadlock exists
        while (true) {
            Integer head = Dependency.isDeadlock();
            if (head == null) {
                break;
            } else {
                System.out.println("Deadlock detected, rolling back transaction " + head);
                Operations.rollback(new Query(head));
            }
        }

        // run waiting transactions that have no dependency
        for (int tID : transactions.keySet()) {
            Set<Integer> deps = Dependency.depGraph.getOrDefault(tID, new HashSet<>());
            if (deps.size() == 0 && transactions.getOrDefault(tID, TransactionStatus.NONE) == TransactionStatus.BLOCK) {
                runWaitingQueries(tID);
            }
        }
    }

    public static void runWaitingQueries(int tID) {
        System.out.println("Executing blocked and pending queries for transaction " + tID);

        // unblock the transaction first
        transactions.put(tID, TransactionStatus.ACTIVE);

        for (int i = 0; i < queries.length; i++) {
            if (queries[i].tID == tID && (queries[i].status == Query.Status.BLOCKED || queries[i].status == Query.Status.PENDING)) {

                queries[i].execute();

                // don't run further if one is blocked
                if (queries[i].status == Query.Status.BLOCKED) {
                    return;
                }
            }
        }
    }
}
