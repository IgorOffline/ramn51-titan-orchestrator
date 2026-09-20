/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

package titan.manual;

import titan.tasks.ProcessRegistry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.List;

/**
 * Covers the two ways a single shared registry can hurt you.
 *
 * Making the journal host-wide is what stops the cleaner from looking in the wrong file, but it
 * also means a starting worker now reads rows belonging to workers that may be alive. And a PID
 * on its own is not safe to kill: numbers get recycled. Both are verified here against real OS
 * processes rather than mocks, because both failures are about real process identity.
 */
public class ProcessRegistryTest {

    private static int passed = 0, failed = 0;

    private static void check(String name, boolean ok) {
        if (ok) { passed++; System.out.println("  PASS  " + name); }
        else    { failed++; System.out.println("  FAIL  " + name); }
    }

    private static Process spawn() throws Exception {
        return new ProcessBuilder("sleep", "120").start();
    }

    private static long startOf(long pid) {
        return ProcessHandle.of(pid).flatMap(h -> h.info().startInstant())
                .map(java.time.Instant::toEpochMilli).orElse(0L);
    }

    public static void main(String[] args) throws Exception {
        File tmp = File.createTempFile("titan_registry_test", ".db");
        tmp.delete();
        System.setProperty("titan.registry.file", tmp.getAbsolutePath());
        System.out.println("registry under test: " + tmp);

        // ---------------------------------------------------------------
        // A live peer's rows must survive another worker's startup cleanup.
        // ---------------------------------------------------------------
        System.out.println("\n[1] a starting worker must not kill a live peer's services");
        Process peer = spawn();      // "owned" by worker 9002, still running
        Process mine = spawn();      // owned by 9001, left behind by its previous incarnation
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmp))) {
            bw.write("9002,svc-peer," + peer.pid() + "," + startOf(peer.pid())); bw.newLine();
            bw.write("9001,svc-mine," + mine.pid() + "," + startOf(mine.pid())); bw.newLine();
        }

        ProcessRegistry.init(9001);
        ProcessRegistry.loadAndCleanUpProcesses();
        Thread.sleep(300);

        check("own leftover process was killed", !mine.isAlive());
        check("live peer's process was NOT killed", peer.isAlive());

        List<String> rows = Files.readAllLines(tmp.toPath());
        check("peer's row preserved in the journal",
                rows.stream().anyMatch(r -> r.startsWith("9002,svc-peer")));
        check("own reaped row removed from the journal",
                rows.stream().noneMatch(r -> r.startsWith("9001,svc-mine")));

        // ---------------------------------------------------------------
        // A recycled PID must not be killed.
        // ---------------------------------------------------------------
        System.out.println("\n[2] a recycled PID must not be killed");
        Process bystander = spawn();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmp))) {
            // Same PID, but a start time from long ago: this is a different process now.
            bw.write("9001,svc-stale," + bystander.pid() + ",1000000000000"); bw.newLine();
            bw.write("9002,svc-peer," + peer.pid() + "," + startOf(peer.pid())); bw.newLine();
        }
        ProcessRegistry.init(9001);
        ProcessRegistry.loadAndCleanUpProcesses();
        Thread.sleep(300);

        check("bystander on a recycled PID survived", bystander.isAlive());
        rows = Files.readAllLines(tmp.toPath());
        check("stale row dropped anyway",
                rows.stream().noneMatch(r -> r.startsWith("9001,svc-stale")));
        check("peer still untouched after second pass", peer.isAlive());

        // ---------------------------------------------------------------
        // register / unregister must not clobber other owners.
        // ---------------------------------------------------------------
        System.out.println("\n[3] register and unregister preserve foreign rows");
        Process svc = spawn();
        ProcessRegistry.register("svc-new", svc.pid());
        rows = Files.readAllLines(tmp.toPath());
        check("new row written with owner tag",
                rows.stream().anyMatch(r -> r.startsWith("9001,svc-new," + svc.pid() + ",")));
        check("row carries a real start time",
                rows.stream().anyMatch(r -> r.startsWith("9001,svc-new,")
                        && !r.endsWith(",0")));
        check("foreign row survived a register", 
                rows.stream().anyMatch(r -> r.startsWith("9002,svc-peer")));

        ProcessRegistry.unregister("svc-new");
        rows = Files.readAllLines(tmp.toPath());
        check("row gone after unregister",
                rows.stream().noneMatch(r -> r.startsWith("9001,svc-new")));
        check("foreign row survived an unregister",
                rows.stream().anyMatch(r -> r.startsWith("9002,svc-peer")));

        // ---------------------------------------------------------------
        // Graceful exit kills the tree and leaves foreign rows alone.
        // ---------------------------------------------------------------
        System.out.println("\n[4] killAllRegistered reaps own trees only");
        Process a = spawn(), b = spawn();
        ProcessRegistry.register("svc-a", a.pid());
        ProcessRegistry.register("svc-b", b.pid());
        int killed = ProcessRegistry.killAllRegistered();
        Thread.sleep(300);
        check("both own services killed", killed == 2 && !a.isAlive() && !b.isAlive());
        check("peer still alive after killAllRegistered", peer.isAlive());
        rows = Files.readAllLines(tmp.toPath());
        check("journal retains only the foreign row",
                rows.size() == 1 && rows.get(0).startsWith("9002,svc-peer"));

        // ---------------------------------------------------------------
        // Corrupt rows must not abort the pass.
        // ---------------------------------------------------------------
        System.out.println("\n[5] corrupt rows are survivable");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmp))) {
            bw.write("garbage"); bw.newLine();
            bw.write("9001,svc-x,notanumber,0"); bw.newLine();
            bw.write("9002,svc-peer," + peer.pid() + "," + startOf(peer.pid())); bw.newLine();
        }
        ProcessRegistry.init(9001);
        ProcessRegistry.loadAndCleanUpProcesses();
        check("cleanup completed despite corrupt rows", true);
        rows = Files.readAllLines(tmp.toPath());
        check("corrupt rows dropped, foreign row kept",
                rows.size() == 1 && rows.get(0).startsWith("9002,svc-peer"));
        check("peer survived the corrupt-row pass", peer.isAlive());

        peer.destroyForcibly();
        bystander.destroyForcibly();
        tmp.delete();
        new File(tmp.getAbsolutePath() + ".lock").delete();

        System.out.println("\n" + passed + "/" + (passed + failed) + " passed");
        if (failed > 0) System.exit(1);
    }
}
