package com.baylor.lock;

import java.util.*;

public class Dependency {
    public static Map<Integer, Set<Integer>> depGraph = new HashMap<>();

    public static void addDependency(int tID, Set<Integer> deps) {
        deps.remove(tID); // don't add self dependency
        Set<Integer> oldDeps = depGraph.getOrDefault(tID, new HashSet<>());
        oldDeps.addAll(deps);
        depGraph.put(tID, oldDeps);
    }

    public static void removeDependency(int tID) {
        depGraph.remove(tID);

        for (int k : depGraph.keySet()) {
            Set<Integer> oldDeps = depGraph.getOrDefault(k, new HashSet<>());
            oldDeps.removeAll(Collections.singleton(tID));
            depGraph.put(k, oldDeps);
        }
    }

    public static Map<Integer, Integer> visited; // 0 = not-visited, 1 = on-stack, 2 = done
    public static int cycleHead;

    public static boolean hasCycle(int u) {
        int status = visited.getOrDefault(u, 0);
        if (status == 1) {
            cycleHead = u;
            return true;
        }
        if (status == 2) {
            return false;
        }

        visited.put(u, 1);

        for (int v : depGraph.getOrDefault(u, new HashSet<>())) {
            if (hasCycle(v)) {
                return true;
            }
        }

        visited.put(u, 2);
        return false;
    }

    public static Integer isDeadlock() {
        visited = new HashMap<>();

        for (int k : depGraph.keySet()) {
            if (hasCycle(k)) {
                return cycleHead;
            }
        }

        return null;
    }
}
