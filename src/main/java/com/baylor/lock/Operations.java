package com.baylor.lock;

import java.util.Collections;
import java.util.HashMap;
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

            handleDeadlock(query.tID);
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

            handleDeadlock(query.tID);
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

        // check if any blocked transaction can be executed
        checkBlocked();
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

        // check if any blocked transaction can be executed
        checkBlocked();
    }

    // release all locks and remove all dependencies
    public static void releaseTransaction(int tID) {
        for (int i = 0; i < 32; i++) {
            lockTable[i].unlock(tID);
        }
        removeDependency(tID);
    }

    public static void addDependency(int tID, Set<Integer> deps) {
        deps.remove(tID); // don't add self dependency
        Set<Integer> oldDeps = depGraph.getOrDefault(tID, new HashSet<>());
        oldDeps.addAll(deps);
        depGraph.put(tID, oldDeps);
    }

    public static void removeDependency(int tID) {
        depGraph.remove(tID);

        for (int k : depGraph.keySet()) {
            Set<Integer> oldDeps = depGraph.getOrDefault(k, new HashSet<>());
            oldDeps.removeAll(Collections.singleton(tID));
            depGraph.put(k, oldDeps);
        }
    }

    public static boolean hasCycle(int u) {
        int status = visited.getOrDefault(u, 0);
        if (status == 1) {
            return true;
        }
        if (status == 2) {
            return false;
        }

        visited.put(u, 1);

        for (int v : depGraph.getOrDefault(u, new HashSet<>())) {
            if (hasCycle(v)) {
                return true;
            }
        }

        visited.put(u, 2);
        return false;
    }

    public static void handleDeadlock(int tID) {
        visited = new HashMap<>();
        if (hasCycle(tID)) {
            System.out.println("Deadlock detected, rolling back transaction " + tID);
            Operations.rollback(new Query(tID));
        }
    }

    public static void checkBlocked() {
        // run waiting transactions that have no dependency
        while (true) {
            boolean found = false;
            for (int tID : transactions.keySet()) {
                Set<Integer> deps = depGraph.getOrDefault(tID, new HashSet<>());
                if (deps.size() == 0 && transactions.getOrDefault(tID, TransactionStatus.NONE) == TransactionStatus.BLOCK) {
                    found = true;
                    runWaitingQueries(tID);
                }
            }
            if (!found) {
                break;
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
