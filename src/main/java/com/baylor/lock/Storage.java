package com.baylor.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Storage {
    public static int data;

    public static Lock[] lockTable = new Lock[32];
    public static Map<Integer, TransactionStatus> transactions = new HashMap<>();
    public static Map<Integer, Set<Integer>> depGraph = new HashMap<>();
    public static Map<Integer, Integer> visited; // 0 = not-visited, 1 = on-stack, 2 = done
    public static Query[] queries;

    public enum TransactionStatus {
        ACTIVE, BLOCK, COMMIT, ROLLBACK, NONE
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

    public static String getDataBits() {
        String binaryString = Integer.toBinaryString(data);
        return String.format("%32s", binaryString).replace(' ', '0');
    }
}
