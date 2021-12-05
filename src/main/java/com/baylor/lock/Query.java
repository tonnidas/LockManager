package com.baylor.lock;

public class Query {
    public enum Command {
        START, READ, WRITE, COMMIT, ROLLBACK, FORCE_ROLLBACK
    }

    public enum Status {
        NEW, PENDING, BLOCKED, DONE, ROLLBACK, SKIPPED
    }

    String line;
    int dID, tID;
    Command command;
    Status status;

    public Query(String line, Command command, int dID, int tID) {
        this.line = line;
        this.command = command;
        this.dID = dID;
        this.tID = tID;
        this.status = Status.NEW;
    }

    public Query(String line, Command command, int tID) {
        this(line, command, -1, tID);
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
        System.out.println(line + " ==> " + msg);
    }

    public void writeInputLog() {
        System.out.println("\nINPUT: " + line);
    }
}
