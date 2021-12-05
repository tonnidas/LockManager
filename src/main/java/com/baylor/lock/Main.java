package com.baylor.lock;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class Main {
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Exactly one argument (input file name) is expected");
            return;
        }

        parseQueries(args[0]);

        Storage.initLockTable();
        executeQueries();

        System.out.println("\nFinal results:");
        Storage.printDataBits();
        Storage.printLockTable();
        Storage.printTransactions();
    }

    public static void executeQueries() {
        for (int i = 0; i < Storage.queries.length; i++) {
            Storage.queries[i].writeInputLog();
            Storage.queries[i].execute();
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
                Storage.initData(Integer.parseInt(line));
                continue;
            }

            String[] parts = line.split("\\s", -1);

            int dID, tID;

            switch (parts[0]) {
                case "Start":
                    tID = Integer.parseInt(parts[1]);
                    queryList.add(new Query(line, Query.Command.START, tID));
                    break;
                case "Read":
                    dID = Integer.parseInt(parts[1]);
                    tID = Integer.parseInt(parts[2]);
                    queryList.add(new Query(line, Query.Command.READ, dID, tID));
                    break;
                case "Write":
                    dID = Integer.parseInt(parts[1]);
                    tID = Integer.parseInt(parts[2]);
                    queryList.add(new Query(line, Query.Command.WRITE, dID, tID));
                    break;
                case "Commit":
                    tID = Integer.parseInt(parts[1]);
                    queryList.add(new Query(line, Query.Command.COMMIT, tID));
                    break;
                case "Rollback":
                    tID = Integer.parseInt(parts[1]);
                    queryList.add(new Query(line, Query.Command.ROLLBACK, tID));
                    break;
                default:
                    System.out.println("Skipped invalid command: " + parts[0] + " at line: " + lineNumber);
            }
        }

        sc.close();

        // store to global queries array
        Storage.queries = queryList.toArray(new Query[0]);
    }
}
