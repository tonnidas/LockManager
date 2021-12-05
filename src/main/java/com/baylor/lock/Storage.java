package com.baylor.lock;

import java.util.*;

public class Storage {
    public static int data;
    public static int dataCommitted;

    public static Lock[] lockTable = new Lock[32];
    public static Map<Integer, TransactionStatus> transactions = new HashMap<>();
    public static Map<Integer, Set<Integer>> depGraph = new HashMap<>();
    public static Map<Integer, Integer> visited; // 0 = not-visited, 1 = on-stack, 2 = done
    public static Query[] queries;

    public enum TransactionStatus {
        ACTIVE, BLOCKED, COMMITTED, ROLLBACK, NONE
    }

    public static void initData(int value) {
        Storage.data = value;
        Storage.dataCommitted = value;
        System.out.println("Data initialized");
        Storage.printDataBits();
    }

    // returns value of k-th bit
    public static int readData(int k) {
        return (data >> k) & 1;
    }

    // flips the k-th bit and returns thr new value
    public static int writeData(int k) {
        data = data ^ (1 << k);
        return readData(k);
    }

    // returns value of k-th bit
    public static int readDataCommitted(int k) {
        return (dataCommitted >> k) & 1;
    }

    // flips the k-th bit and returns thr new value
    public static int writeDataCommitted(int k) {
        dataCommitted = dataCommitted ^ (1 << k);
        return readDataCommitted(k);
    }

    public static void printDataBits() {
        String binaryString = String.format("%32s", Integer.toBinaryString(data)).replace(' ', '0');
        System.out.println("Dirty data: " + binaryString + " Int value: " + data);

        binaryString = String.format("%32s", Integer.toBinaryString(dataCommitted)).replace(' ', '0');
        System.out.println("Committed data: " + binaryString + " Int value: " + dataCommitted);
    }

    public static void initLockTable() {
        for (int i = 0; i < 32; i++) {
            Storage.lockTable[i] = new Lock();
        }
    }

    public static void printLockTable() {
        System.out.println("\nLock Table:");

        for (int i = 0; i < 32; i++) {
            Lock lock = Storage.lockTable[i];

            List<String> holds = new ArrayList<>();
            for (int t : lock.lockHolds) {
                String mode = lock.lockStatus == Lock.LockStatus.READ ? "Shared" : "Exclusive";
                holds.add("(tID: " + t + ", mode: " + mode + ")");
            }

            List<String> waits = new ArrayList<>();
            for (Lock.LockRequest w : lock.waitingLocks) {
                String mode = w.isRead ? "Shared" : "Exclusive";
                waits.add("(tID: " + w.tID + ", mode: " + mode + ")");
            }

            System.out.println("DataID: " + i + " " + "HoldBy: " + holds + " Waiting: " + waits);
        }
    }

    public static void printTransactions() {
        System.out.println("\nTransactions:");

        for (int tID : Storage.transactions.keySet()) {
            List<String> holds = new ArrayList<>();
            List<String> waits = new ArrayList<>();

            for (int i = 0; i < 32; i++) {
                if (Storage.lockTable[i].lockHolds.contains(tID)) {
                    String mode = Storage.lockTable[i].lockStatus == Lock.LockStatus.READ ? "Shared" : "Exclusive";
                    holds.add("(DataID: " + i + ", mode: " + mode + ")");
                } else {
                    for (Lock.LockRequest w : Storage.lockTable[i].waitingLocks) {
                        if (w.tID == tID) {
                            String mode = w.isRead ? "Shared" : "Exclusive";
                            waits.add("(DataID: " + i + ", mode: " + mode + ")");
                        }
                    }
                }
            }

            System.out.println("tID: " + tID + " " + "Status: " + Storage.transactions.get(tID) + " Holds: " + holds + " Waiting: " + waits);
        }
    }

    public static void printDepGraph() {
        System.out.println("\nDependency Graph:");

        for (int tID : Storage.depGraph.keySet()) {
            System.out.println(tID + ": " + Storage.depGraph.get(tID));
        }
    }
}
