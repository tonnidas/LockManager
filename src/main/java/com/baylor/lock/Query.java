package com.baylor.lock;

public class Query {
    public enum Command {
        START, READ, WRITE, COMMIT, ROLLBACK, FORCE_ROLLBACK
    }

    public enum Status {
        NEW, PENDING, BLOCKED, DONE, ROLLBACK, SKIPPED
    }

    int lineNumber;
    String line;
    int dID, tID;
    Command command;
    Status status;

    public Query(int lineNumber, String line, Command command, int dID, int tID) {
        this.lineNumber = lineNumber;
        this.line = line;
        this.command = command;
        this.dID = dID;
        this.tID = tID;
        this.status = Status.NEW;
    }

    public Query(int lineNumber, String line, Command command, int tID) {
        this(lineNumber, line, command, -1, tID);
    }

    public Query(int tID) {
        this(-1, "", Command.FORCE_ROLLBACK, -1, tID);
    }

    public void execute() {
        switch (command) {
            case START:
                Operations.start(this);
                break;
            case READ:
                Operations.read(this);
                break;
            case WRITE:
                Operations.write(this);
                break;
            case COMMIT:
                Operations.commit(this);
                break;
            case ROLLBACK:
                Operations.rollback(this);
                break;
        }
    }

    public void writeLog(String msg) {
        if (command != Command.FORCE_ROLLBACK) {
            System.out.println("Line " + lineNumber + ": " + line + " ==> " + msg);
        } else {
            System.out.println(msg);
        }
    }

    public void writeInputLog() {
        System.out.println("\nINPUT: " + "Line " + lineNumber + ": " + line + "\n");
    }
}
