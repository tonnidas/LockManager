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
        if (lockStatus == LockStatus.NONE) {
            // granted, no lock
            grantLock(tID, LockStatus.READ);
            return true;
        } else if (lockStatus == LockStatus.READ && lockHolds.contains(tID)) {
            // granted, read lock was hold by the same transaction
            grantLock(tID, LockStatus.READ);
            return true;
        } else if (lockStatus == LockStatus.READ && waitingLocks.isEmpty()) {
            // only read lock and there is no waiting locks, second check is done to avoid starvation
            grantLock(tID, LockStatus.READ);
            return true;
        } else if (lockStatus == LockStatus.WRITE && lockHolds.contains(tID)) {
            // granted, write lock was hold by the same transaction
            // lockStatus should remain WRITE is holding lock was write
            grantLock(tID, LockStatus.WRITE);
            return true;
        } else { // waiting
            addWaiting(tID, true);
            return false;
        }
    }

    public boolean requestWriteLock(int tID) {
        if (lockStatus == LockStatus.NONE) {
            // granted, no lock
            grantLock(tID, LockStatus.WRITE);
            return true;
        } else if (lockStatus == LockStatus.READ && lockHolds.size() == 1 && lockHolds.contains(tID)) {
            // granted, read lock was hold by the same transaction and there is only one lockHolds, upgrade lock
            grantLock(tID, LockStatus.WRITE);
            return true;
        } else if (lockStatus == LockStatus.WRITE && lockHolds.contains(tID)) {
            // granted, write lock was hold by the same transaction
            grantLock(tID, LockStatus.WRITE);
            return true;
        } else { // waiting
            addWaiting(tID, false);
            return false;
        }
    }

    public void grantLock(int tID, LockStatus type) {
        waitingLocks.removeIf(w -> w.tID == tID); // remove from waitingLocks
        lockStatus = type;
        lockHolds.add(tID);
    }

    public void unlock(int tID) {
        waitingLocks.removeIf(w -> w.tID == tID); // remove from waitingLocks
        lockHolds.remove(tID); // remove from lockHolds

        if (lockHolds.size() == 0) {
            lockStatus = LockStatus.NONE;
        }
    }

    public void addWaiting(int tID, boolean isRead) {
        // skip if there is compatible waiting request for the same tID
        // dependency = lock holds + waiting locks, except both current operation and waiting operation is read
        Set<Integer> waitsFor = new HashSet<>(lockHolds);
        for (LockRequest w : waitingLocks) {
            if (w.tID == tID && (w.isRead == isRead || !w.isRead)) {
                return; // compatible lock for the same tID
            }
            if (isRead && w.isRead) {
                continue; // both read
            }
            waitsFor.add(w.tID);
        }

        Operations.addDependency(tID, waitsFor);
        waitingLocks.add(new LockRequest(tID, isRead));
    }
}
