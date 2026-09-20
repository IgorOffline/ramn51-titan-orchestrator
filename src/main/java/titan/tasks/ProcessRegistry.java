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

package titan.tasks;
// This is for cleaning up processes that were zombied due to abrupt worker node shutdown/crash.

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A kill list for long-lived child processes, so that none of them outlive the worker that
 * started them.
 *
 * <h2>What this is not</h2>
 * This registry has nothing to do with recovering <em>jobs</em>. If a worker dies holding
 * in-flight work, the Master notices, calls {@code recoverJobsFrom} and re-queues that work
 * from its own {@code runningJobs} and {@code executionHistory}. That path never reads this
 * file. What it cannot do is reach onto the dead worker's machine and clean up the OS
 * processes left behind, because those are only known locally. That is this file's entire job:
 * it records PIDs so the next worker on that machine can kill what the last one abandoned.
 *
 * <h2>One file, many workers</h2>
 * The path is absolute. It used to be the bare relative name {@code titan_tasks.db}, which
 * resolved against each worker's working directory, so autoscaled workers wrote to a different
 * file than manually started ones. Three registries existed on one host; the cleaner ran
 * faithfully on every startup and looked in the wrong one, which is how services survived for
 * days.
 *
 * <p>Because every worker on a host now shares one file, entries carry the id of the worker
 * that owns them, and a worker only ever reaps <em>its own</em> rows. Without that, a worker
 * starting on port 8081 would read the same file a live worker on 8080 is using and kill its
 * running services. Worker identity is the port: it is what is stable across a restart in the
 * same place, which is exactly the case the cleanup exists for.
 *
 * <p>Concurrent writers are serialised on a sidecar lock file, so a read-modify-write from one
 * worker cannot interleave with another's and drop rows.
 */
public class ProcessRegistry {

    /** serviceId to PID, for processes this worker owns right now. */
    private static final Map<String, Long> persistentMap = new ConcurrentHashMap<>();

    /**
     * serviceId to the process's start time, in epoch millis.
     *
     * A PID alone is not a safe thing to kill later. PIDs are recycled, and on a busy host the
     * wrap-around is fast, so a stale row can name a process that is very much alive and has
     * nothing to do with Titan. Recording when the process started makes the row verifiable:
     * at cleanup time the candidate must still report the same start instant, otherwise it is a
     * different process wearing a reused number and is left alone.
     */
    private static final Map<String, Long> startTimes = new ConcurrentHashMap<>();

    /**
     * Identifies the worker that owns the rows this JVM writes. Set once at startup to the
     * worker's port, which is stable across restarts in the same place.
     */
    private static volatile String ownerId = "unknown";

    /** Resolved once, so a run can never end up split across two files. */
    private static volatile File registryFile;

    /** Called once at worker startup, before {@link #loadAndCleanUpProcesses()}. */
    public static void init(int workerPort) {
        ownerId = String.valueOf(workerPort);
        registryFile = null;   // re-resolve, in case configuration was set programmatically
        System.out.println("[INFO] [REGISTRY] Owner " + ownerId + ", journal " + registryFile());
    }

    /**
     * The one registry file for this host. Absolute, so it does not matter what directory a
     * worker happens to be started from. Override with {@code -Dtitan.registry.file=...} or
     * {@code TITAN_REGISTRY_FILE}.
     *
     * <p>System properties are consulted directly because {@code TitanConfig} reads only the
     * environment and {@code titan.properties}; a {@code -D} override would otherwise be
     * accepted on the command line and silently ignored.
     */
    private static File registryFile() {
        File cached = registryFile;
        if (cached != null) return cached;
        synchronized (ProcessRegistry.class) {
            if (registryFile != null) return registryFile;
            String configured = System.getProperty("titan.registry.file");
            if (configured == null || configured.isBlank()) {
                configured = titan.TitanConfig.get("titan.registry.file", null);
            }
            File f;
            if (configured != null && !configured.isBlank()) {
                f = new File(configured).getAbsoluteFile();
            } else {
                File dir = new File(System.getProperty("user.home"), ".titan");
                if (!dir.exists()) dir.mkdirs();
                f = new File(dir, "titan_tasks.db");
            }
            File parent = f.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            registryFile = f;
            return f;
        }
    }

    /** A row: owner, serviceId, pid, start time. */
    private static String row(String serviceId) {
        return ownerId + "," + serviceId + "," + persistentMap.get(serviceId)
                + "," + startTimes.getOrDefault(serviceId, 0L);
    }

    /**
     * Reads every row, hands each one to the caller, and writes back whatever the caller keeps,
     * all while holding the host-wide lock. Both cleanup and save are this same operation with a
     * different rule for what survives, and doing it under one lock is what stops two workers
     * from interleaving a read-modify-write and losing each other's rows.
     */
    private static synchronized void withRegistry(java.util.function.Predicate<String[]> keepForeign,
                                                  boolean writeOwnRows) {
        File f = registryFile();
        File lockFile = new File(f.getParentFile(), f.getName() + ".lock");
        try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
             FileChannel ch = raf.getChannel();
             FileLock ignored = ch.lock()) {

            List<String> survivors = new ArrayList<>();
            if (f.exists()) {
                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isBlank()) continue;
                        String[] parts = line.split(",");
                        if (parts.length != 4) {
                            System.err.println("[WARN] [REGISTRY] Corrupt row, dropping: " + line);
                            continue;
                        }
                        // Another worker's rows are never touched: it may well be alive.
                        if (!parts[0].equals(ownerId)) {
                            survivors.add(line);
                        } else if (keepForeign != null && keepForeign.test(parts)) {
                            survivors.add(line);
                        }
                    }
                }
            }
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(f, false))) {
                for (String s : survivors) {
                    bw.write(s);
                    bw.newLine();
                }
                if (writeOwnRows) {
                    for (String serviceId : persistentMap.keySet()) {
                        bw.write(row(serviceId));
                        bw.newLine();
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("[ERROR] [REGISTRY] " + f + ": " + e.getMessage());
        }
    }

    /**
     * Kills whatever this worker's previous incarnation left running, then takes ownership of a
     * clean slate. Rows belonging to other workers on the same host are preserved untouched.
     *
     * <p>A row is only acted on if the PID is still alive <em>and</em> still reports the start
     * time that was recorded for it. That second condition is what makes this safe to run on a
     * busy machine.
     */
    public static void loadAndCleanUpProcesses() {
        File f = registryFile();
        if (!f.exists()) return;
        System.out.println("[INFO] [ZOMBIE KILLER] Checking for processes left by a previous worker "
                + ownerId + "...");
        withRegistry(parts -> {
            String serviceId = parts[1];
            long pid;
            long recordedStart;
            try {
                pid = Long.parseLong(parts[2]);
                recordedStart = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                return false;   // unparseable, drop it
            }
            Optional<ProcessHandle> handle = ProcessHandle.of(pid);
            if (handle.isEmpty() || !handle.get().isAlive()) return false;   // already gone

            ProcessHandle ph = handle.get();
            long actualStart = ph.info().startInstant().map(java.time.Instant::toEpochMilli).orElse(-1L);
            // Tolerate a second of slop: the JVM and the OS do not always agree to the millisecond.
            if (recordedStart > 0 && actualStart > 0 && Math.abs(actualStart - recordedStart) > 1000) {
                System.out.println("[INFO] [ZOMBIE KILLER] PID " + pid + " (" + serviceId
                        + ") was recycled and is now a different process. Leaving it alone.");
                return false;
            }
            System.out.println("[INFO] [ZOMBIE KILLER] Found zombie (PID " + pid + ") - "
                    + serviceId + ". Killing its process tree.");
            ph.descendants().forEach(ProcessHandle::destroyForcibly);
            ph.destroyForcibly();
            return false;   // handled, so the row goes
        }, false);
    }

    /**
     * Records a process this worker started, and flushes it to the shared journal immediately.
     * Immediately matters: the case the journal exists for is a worker being killed without
     * warning, and anything not yet written at that moment is lost.
     *
     * @param serviceId unique identifier for the service.
     * @param pid the OS process id.
     */
    public static void register(String serviceId, long pid) {
        persistentMap.put(serviceId, pid);
        startTimes.put(serviceId,
                ProcessHandle.of(pid)
                        .flatMap(h -> h.info().startInstant())
                        .map(java.time.Instant::toEpochMilli)
                        .orElse(0L));
        withRegistry(null, true);
    }

    /**
     * Forgets a process that has exited normally, so no later cleanup pass considers its PID.
     *
     * @param serviceId the service to forget.
     */
    public static void unregister(String serviceId) {
        persistentMap.remove(serviceId);
        startTimes.remove(serviceId);
        withRegistry(null, true);
    }

    /**
     * Kills every process this worker started, process trees included, and clears its rows.
     * Called when a worker exits under its own control, so that the common case leaves nothing
     * behind for a later cleanup pass to find.
     *
     * @return how many process trees were killed.
     */
    public static int killAllRegistered() {
        int n = 0;
        for (Map.Entry<String, Long> e : persistentMap.entrySet()) {
            Optional<ProcessHandle> h = ProcessHandle.of(e.getValue());
            if (h.isPresent() && h.get().isAlive()) {
                h.get().descendants().forEach(ProcessHandle::destroyForcibly);
                h.get().destroyForcibly();
                System.out.println("[SHUTDOWN] Killed service tree " + e.getKey()
                        + " (pid " + e.getValue() + ")");
                n++;
            }
        }
        persistentMap.clear();
        startTimes.clear();
        withRegistry(null, true);
        return n;
    }
}
