package com.baylor.lock;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static com.baylor.lock.Operations.organize;
import static com.baylor.lock.Storage.lockTable;

public class Main {
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Exactly one argument (input file name) is expected");
            return;
        }

        parseQueries(args[0]);

        initLockTable();
        executeQueries();

        System.out.println("\nFinal data value: " + Storage.data);
        printLockTable();
        printTransactions();
    }

    public static void executeQueries() {
        for (int i = 0; i < Storage.queries.length; i++) {
            Storage.queries[i].writeLog("-------Processing-------");
            Storage.queries[i].execute();
            organize();
        }
    }

    public static void parseQueries(String inputFile) throws FileNotFoundException {
        List<Query> queryList = new ArrayList<>();
        Scanner sc = new Scanner(new File(inputFile));
        int lineNumber = 0;

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            lineNumber++;

            if (line.equals("EOF")) {
                break;
            }

            // first line of the input is the initial value
            if (lineNumber == 1) {
                Storage.data = Integer.parseInt(line);
                System.out.println("Data initialized to " + Storage.getDataBits());
                continue;
            }

            String[] parts = line.split("\\s", -1);

            int dID, tID;

            switch (parts[0]) {
                case "Start":
                    tID = Integer.parseInt(parts[1]);
                    queryList.add(new Query(lineNumber, line, Query.Command.START, tID));
                    break;
                case "Read":
                    dID = Integer.parseInt(parts[1]);
                    tID = Integer.parseInt(parts[2]);
                    queryList.add(new Query(lineNumber, line, Query.Command.READ, dID, tID));
                    break;
                case "Write":
                    dID = Integer.parseInt(parts[1]);
                    tID = Integer.parseInt(parts[2]);
                    queryList.add(new Query(lineNumber, line, Query.Command.WRITE, dID, tID));
                    break;
                case "Commit":
                    tID = Integer.parseInt(parts[1]);
                    queryList.add(new Query(lineNumber, line, Query.Command.COMMIT, tID));
                    break;
                case "Rollback":
                    tID = Integer.parseInt(parts[1]);
                    queryList.add(new Query(lineNumber, line, Query.Command.ROLLBACK, tID));
                    break;
                default:
                    System.out.println("Skipped invalid command: " + parts[0] + " at line: " + lineNumber);
            }
        }

        sc.close();

        // store to global queries array
        Storage.queries = queryList.toArray(new Query[0]);
    }

    public static void initLockTable() {
        for (int i = 0; i < 32; i++) {
            Storage.lockTable[i] = new Lock();
        }
    }

    public static void printLockTable() {
        System.out.println("\nLock Table:");
        for (int i = 0; i < 32; i++) {
            System.out.println("DataID: " + i + " " + printLockStatus(lockTable[i]));
        }
    }

    public static void printTransactions() {
        System.out.println("\nTransactions:");
        for (int tID : Storage.transactions.keySet()) {
            System.out.println("tID: " + tID + " " + printTransactionStatus(tID));
        }
    }

    public static String printLockStatus(Lock lock) {
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

        return "HoldBy: " + holds + " Waiting: " + waits;
    }

    public static String printTransactionStatus(int tID) {
        List<String> holds = new ArrayList<>();
        List<String> waits = new ArrayList<>();

        for (int i = 0; i < 32; i++) {
            if (lockTable[i].lockHolds.contains(tID)) {
                String mode = lockTable[i].lockStatus == Lock.LockStatus.READ ? "Shared" : "Exclusive";
                holds.add("(DataID: " + i + ", mode: " + mode + ")");
            } else {
                for (Lock.LockRequest w : lockTable[i].waitingLocks) {
                    if (w.tID == tID) {
                        String mode = w.isRead ? "Shared" : "Exclusive";
                        waits.add("(DataID: " + i + ", mode: " + mode + ")");
                    }
                }
            }
        }

        return "Status: " + Storage.transactions.get(tID) + " Holds: " + holds + " Waiting: " + waits;
    }
}