package com.baylor.lock;

import java.util.*;

public class Storage {
    private static long data;
    private static long dataCommitted;

    public static Lock[] lockTable = new Lock[32];
    public static Map<Integer, TransactionStatus> transactions = new HashMap<>();
    public static Map<Integer, Set<Integer>> depGraph = new HashMap<>();
    public static Map<Integer, Integer> visited; // 0 = not-visited, 1 = on-stack, 2 = done
    public static Query[] queries;

    public enum TransactionStatus {
        ACTIVE, BLOCKED, COMMITTED, ROLLBACK, NONE
    }

    public static void initData(long value) {
        Storage.data = value;
        Storage.dataCommitted = value;
        System.out.println("Initial data: " + binaryString(data));
    }

    // returns value of k-th bit
    public static int readData(int k) {
        return (int) ((data >> k) & 1);
    }

    // flips the k-th bit and returns thr new value
    public static int writeData(int k) {
        data = data ^ (1L << k);
        return readData(k);
    }

    // returns value of k-th bit
    public static int readDataCommitted(int k) {
        return (int) ((dataCommitted >> k) & 1);
    }

    // flips the k-th bit and returns thr new value
    public static int writeDataCommitted(int k) {
        dataCommitted = dataCommitted ^ (1L << k);
        return readDataCommitted(k);
    }

    public static void initLockTable() {
        for (int i = 0; i < 32; i++) {
            Storage.lockTable[i] = new Lock();
        }
    }

    public static String binaryString(long value) {
        return String.format("%32s", Long.toBinaryString(value)).replace(' ', '0') + ", Int value: " + value;
    }

    public static void printDataBits() {
        System.out.println("Dirty data: " + binaryString(data));
        System.out.println("Committed data: " + binaryString(dataCommitted));
    }

    public static void printLockTable() {
        System.out.println("\nLock Table: (Mode: R = Read/Shared, W = Write/Exclusive) (Skipped Empty)");

        for (int i = 0; i < 32; i++) {
            Lock lock = Storage.lockTable[i];

            if (lock.lockHolds.isEmpty() && lock.waitingLocks.isEmpty()) {
                continue;
            }

            List<String> holds = new ArrayList<>();
            for (int t : lock.lockHolds) {
                String mode = lock.lockStatus == Lock.LockStatus.READ ? "R" : "W";
                holds.add(t + "/" + mode);
            }

            List<String> waits = new ArrayList<>();
            for (Lock.LockRequest w : lock.waitingLocks) {
                String mode = w.isRead ? "R" : "W";
                waits.add(w.tID + "/" + mode);
            }

            System.out.println("DataID: " + i + ", " + "HoldBy (TrID/Mode): " + holds + ", Waiting (TrID/Mode): " + waits);
        }
    }

    public static void printTransactions() {
        System.out.println("\nTransactions: (Mode: R = Read/Shared, W = Write/Exclusive)");

        for (int tID : Storage.transactions.keySet()) {
            List<String> holds = new ArrayList<>();
            List<String> waits = new ArrayList<>();

            for (int i = 0; i < 32; i++) {
                if (Storage.lockTable[i].lockHolds.contains(tID)) {
                    String mode = Storage.lockTable[i].lockStatus == Lock.LockStatus.READ ? "R" : "W";
                    holds.add(i + "/" + mode);
                } else {
                    for (Lock.LockRequest w : Storage.lockTable[i].waitingLocks) {
                        if (w.tID == tID) {
                            String mode = w.isRead ? "R" : "W";
                            waits.add(i + "/" + mode);
                        }
                    }
                }
            }

            System.out.println("TrID: " + tID + ", Status: " + Storage.transactions.get(tID) + ", Holding (DataID/Mode): " + holds
                    + ", Waiting (DataID/Mode): " + waits + ", Dependencies (TrID): " + depGraph.getOrDefault(tID, new HashSet<>()));
        }
    }
}
