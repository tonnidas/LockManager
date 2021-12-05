package com.baylor.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Storage {
    public static int data;
    public static int dataCommitted;

    public static Lock[] lockTable = new Lock[32];
    public static Map<Integer, TransactionStatus> transactions = new HashMap<>();
    public static Map<Integer, Set<Integer>> depGraph = new HashMap<>();
    public static Map<Integer, Integer> visited; // 0 = not-visited, 1 = on-stack, 2 = done
    public static Query[] queries;

    public enum TransactionStatus {
        ACTIVE, BLOCK, COMMIT, ROLLBACK, NONE
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
        return readData(k);
    }

    public static void printDataBits() {
        String binaryString = String.format("%32s", Integer.toBinaryString(data)).replace(' ', '0');
        System.out.println("Dirty data: " + binaryString + " Int value: " + data);

        binaryString = String.format("%32s", Integer.toBinaryString(dataCommitted)).replace(' ', '0');
        System.out.println("Committed data: " + binaryString + " Int value: " + dataCommitted);
    }
}
