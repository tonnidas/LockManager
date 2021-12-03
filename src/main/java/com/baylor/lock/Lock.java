package com.baylor.lock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Lock {
    public enum LockStatus {
        NONE, READ, WRITE
    }

    public static class LockRequest {
        int tID;
        boolean isRead;

        public LockRequest(int tID, boolean isRead) {
            this.tID = tID;
            this.isRead = isRead;
        }
    }

    // if lockStatus = NONE, lockHolds is empty
    // if lockStatus = READ, lockHolds can contain multiple tID
    // if lockStatus = WRITE, lockHolds contains only one tID

    LockStatus lockStatus;
    Set<Integer> lockHolds;
    List<LockRequest> waitingLocks;

    public Lock() {
        lockStatus = LockStatus.NONE;
        lockHolds = new HashSet<>();
        waitingLocks = new ArrayList<>();
    }

    public boolean requestReadLock(int tID) {
        if (lockStatus == LockStatus.NONE || lockStatus == LockStatus.READ) {
            // granted, no lock or read lock
            lockStatus = LockStatus.READ;
            lockHolds.add(tID);
            return true;
        } else if (lockHolds.contains(tID)) {
            // lockStatus should remain WRITE
            return true;
        } else { // waiting
            addWaiting(tID, true);
            return false;
        }
    }

    public boolean requestWriteLock(int tID) {
        if (lockStatus == LockStatus.NONE) {
            // granted, no lock
            lockStatus = LockStatus.WRITE;
            lockHolds.add(tID);
            return true;
        } else if (lockStatus == LockStatus.READ && lockHolds.size() == 1 && lockHolds.contains(tID)) {
            // granted, read lock was hold by the same transaction, upgrade lock
            lockStatus = LockStatus.WRITE;
            return true;
        } else if (lockStatus == LockStatus.WRITE && lockHolds.contains(tID)) {
            // granted, write lock was hold by the same transaction
            // lockStatus should remain WRITE
            return true;
        } else { // waiting
            addWaiting(tID, false);
            return false;
        }
    }

    public void unlock(int tID) {
        waitingLocks.removeIf(w -> w.tID == tID); // remove from waitingLocks
        lockHolds.remove(tID); // remove from lockHolds

        if (lockHolds.size() == 0) {
            lockStatus = LockStatus.NONE;
        }
    }

    // if there is existing waiting WRITE lock for the same tID, do nothing
    // if there is existing waiting READ lock for the same tID, do nothing if current request is READ, else make it WRITE
    public void addWaiting(int tID, boolean isRead) {
        int i = 0;
        for (LockRequest w : waitingLocks) {
            if (w.tID == tID) {
                if (w.isRead && !isRead) {
                    w.isRead = false;
                    waitingLocks.set(i, w);
                }
                return;
            }
            i++;
        }

        // no existing entry, add new entry
        waitingLocks.add(new LockRequest(tID, isRead));
        Dependency.addDependency(tID, lockHolds);
    }
}
