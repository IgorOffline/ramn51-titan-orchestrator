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

import titan.network.RpcWorkerServer;
import titan.tasks.TaskHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import titan.network.LogBatcher;

/**
 * {@code ScriptExecutorHandler} is an implementation of {@link TaskHandler} responsible for executing scripts
 * (Python, Shell, or standalone executables) on the worker node.
 * It manages a dedicated workspace for script execution, handles payload parsing,
 * process execution, real-time log streaming, and result reporting back to the master.
 * <p>
 * This handler supports different execution contexts:
 * <ul>
 *     <li><b>Isolated:</b> Each job gets its own subdirectory within the root workspace.</li>
 *     <li><b>Shared:</b> A common 'shared' directory for DAG-related tasks.</li>
 *     <li><b>Archive Mode:</b> Executes scripts directly within their unzipped absolute path.</li>
 * </ul>
 * It also provides robust parsing for job payloads, supporting both new (JOB_ID | FILENAME | ARGS)
 * and legacy formats.
 */
    public class ScriptExecutorHandler implements TaskHandler {
    /**
     * Registry of actively running script processes, keyed by job ID.
     * Populated on process start and cleared on completion/failure.
     * Used by {@link #killJob(String)} to forcibly terminate a running script.
     */
    private static final java.util.concurrent.ConcurrentHashMap<String, Process> activeProcesses =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Kills a process and everything it spawned.
     *
     * destroyForcibly() only signals the direct child. A task that starts its own subprocesses
     * leaves them running when it dies, and they are then re-parented to init where nothing owns
     * them: the slot is freed while the work continues, so the fleet's real load no longer matches
     * what the Master believes. Measured directly: a cancelled job's grandchild was still alive
     * four seconds later.
     *
     * Descendants are killed BEFORE the parent. Killing the parent first re-parents its children
     * immediately, and the handle needed to find them is gone.
     */
    private static void killTree(Process p) {
        if (p == null) return;
        try {
            p.toHandle().descendants().forEach(ProcessHandle::destroyForcibly);
        } catch (Exception ignored) {
            // descendants() is best-effort; never let cleanup failure mask the kill below
        }
        p.destroyForcibly();
    }

    /**
     * Kills every task still running on this worker, process trees included.
     *
     * Called when a worker is about to exit on its own. Leaving the task processes behind would
     * recreate exactly the orphan problem the exit is meant to avoid: they would be re-parented to
     * init with no worker and no Master aware of them.
     *
     * @return how many jobs were killed.
     */
    public static int killAll() {
        int n = 0;
        for (java.util.Map.Entry<String, Process> e : activeProcesses.entrySet()) {
            killTree(e.getValue());
            System.out.println("[SHUTDOWN] Killed process tree for job: " + e.getKey());
            n++;
        }
        activeProcesses.clear();
        return n;
    }

    /**
     * Forcibly terminates the process associated with the given job ID.
     * @return {@code true} if a process was found and destroyed, {@code false} if not found.
     */
    public static boolean killJob(String jobId) {
        Process p = activeProcesses.get(jobId);
        if (p != null) {
            killTree(p);
            System.out.println("[CANCEL] Killed process for job: " + jobId);
            return true;
        }
        System.out.println("[CANCEL] No active process found for job: " + jobId);
        return false;
    }

    /**
     * The base directory name for all Titan worker workspaces.
     * All job-specific and shared workspaces will be created under this directory.
     */
    private static final String WORKSPACE_DIR = "titan_workspace";
    /**
     * A reference to the parent {@link RpcWorkerServer} instance.
     * Used for interacting with the master, specifically for streaming logs.
     */
    private final RpcWorkerServer parentServer;

    /**
     * The root {@link File} object representing the base directory for all script executions.
     * This is typically {@code titan_workspace}.
     */
    private final File rootWorkspace;
    /**
     * The {@link File} object representing the shared workspace directory.
     * This directory is used for tasks that require shared resources, such as DAG executions.
     * It is located at {@code titan_workspace/shared}.
     */
    private final File sharedWorkspace;

    /**
     * Constructs a new {@code ScriptExecutorHandler}.
     * Initializes the root and shared workspace directories, creating them if they do not exist.
     *
     * @param parentServer The {@link RpcWorkerServer} instance that this handler is associated with.
     */
    public ScriptExecutorHandler(RpcWorkerServer parentServer) {
        this.parentServer = parentServer;

        this.rootWorkspace = new File(WORKSPACE_DIR);
        if (!rootWorkspace.exists()) {
            boolean created = rootWorkspace.mkdirs();
            if(created) System.out.println("[INIT] Created Root Workspace: " + rootWorkspace.getAbsolutePath());
        }

        this.sharedWorkspace = new File(rootWorkspace, "shared");
        if (!sharedWorkspace.exists()) {
            sharedWorkspace.mkdirs();
            System.out.println("[INIT] Created Shared DAG Workspace: " + sharedWorkspace.getAbsolutePath());
        }
    }

    /**
     * Executes a script based on the provided payload.
     * The payload is parsed to extract the job ID, script filename, and any arguments.
     * The script is then executed in an appropriate workspace directory (isolated, shared, or archive mode).
     * Standard output and error streams of the script are captured and streamed to the master via a {@link LogBatcher}.
     * The method waits for the script to complete or times out after 60 seconds.
     *
     * <p>Payload Format (New): {@code JOB_ID | FILENAME | ARGS}
     * <p>Payload Format (Legacy): {@code FILENAME | ... | JOB_ID} (where ARGS are not explicitly passed)
     *
     * @param payload The string payload containing script execution details.
     * @return A string indicating the execution status and result. Format: {@code COMPLETED|ExitCode|OutputContent}
     *         or {@code ERROR: [message]} if an error occurs or the script times out.
     */
    @Override
    public String execute(String payload) {
        // ROBUST PARSING (Standardized Format: JOB_ID | FILENAME | ARGS)
        // We limit split to 3 so that arguments containing pipes are retained
        String[] parts = payload.split("\\|", 3);

        String jobId = "UNKNOWN";
        String filename = "";
        String args = "";

//        String filename = parts[0];
//        System.out.println("GIVEN PAYLOAD TO SCRIPT EXECUTOR " + payload);
//
//        String jobId = filename;
        if (parts.length >= 2 && (parts[1].endsWith(".py") || parts[1].endsWith(".sh"))) {
            // It is likely NEW format: JOB-123 | script.py | args
            jobId = parts[0];
            filename = parts[1];
            if (parts.length > 2) args = parts[2];
        }
        // This way I am just maintaining the previous crap parsing just for backward compatability where args are not passed
        // OF: (Filename | ... | ID)
        // Args remain null if old format is used
        else {
            filename = parts[0];
            // Handle RUN_PAYLOAD prefix edge case
            if (filename.equals("RUN_PAYLOAD") && parts.length > 1) {
                filename = parts[1];
            }

            // Extract Job ID from the LAST part
            if (parts.length > 1) {
                String lastPart = parts[parts.length - 1];
                if (!lastPart.equals(filename) && !lastPart.equals("RUN_PAYLOAD")) {
                    jobId = lastPart;
                }
            }
        }

        System.out.println("[INFO] Parsed -> Job: " + jobId + " | File: " + filename + " | Args: " + args);
        System.out.println("[INFO] [ScriptExecutor] Running: " + filename + " (ID: " + jobId + ")");
//        File scriptFile = new File(WORKSPACE_DIR + File.separator + filename);
//        File scriptFile = new File(rootWorkspace, filename);
        File scriptFile = new File(filename);

        if (!scriptFile.isAbsolute()) {
            // If it's relative (Normal execution), prepend the workspace root
            scriptFile = new File(rootWorkspace, filename);
        }

        if (!scriptFile.exists()) {
            return "ERROR: Script file not found: " + filename;
        }

        LogBatcher batcher = new LogBatcher(
                jobId,
                parentServer.getSchedulerHost(),
                parentServer.getSchedulerPort()
        );

        try {

            File executionDir;
            // 3. Selection Logic (No redundant mkdirs for Shared)
            if (scriptFile.isAbsolute()) {
                // ARCHIVE MODE: Run inside the unzipped folder (like in titan_workspace/JOB-123/)
                executionDir = scriptFile.getParentFile();
            }
            else if (jobId.startsWith("DAG-")) {
                // Use the pre-calculated shared folder
                executionDir = this.sharedWorkspace;
            } else {
                // ISOLATED: This MUST be created on the run, as it is unique to this job
                executionDir = new File(rootWorkspace, jobId);
                if (!executionDir.exists()) executionDir.mkdirs();
            }

            List<String> command = new ArrayList<>();

            if(filename.endsWith(".py")){
                command.add("python");
                command.add("-u");
                command.add(scriptFile.getAbsolutePath());
            } else if(filename.endsWith(".sh")){
                command.add("/bin/bash");
                command.add(scriptFile.getAbsolutePath());
            } else {
                command.add(scriptFile.getAbsolutePath()); // This is the case for Executable binary file
            }

            if(!args.isEmpty()){
                String [] argList = args.split(" ");
                for(String arg: argList){
                    if(!arg.trim().isEmpty()){
                        command.add(arg.trim());
                    }
                }
            }

            ProcessBuilder pb = new ProcessBuilder(command);

            pb.directory(executionDir);
            // Combine Errors with Output
            pb.redirectErrorStream(true);

            System.out.println("[INFO] Context: " + executionDir.getName());
            System.out.println("[INFO] Executing Command: " + command);

            // 3. Start Process
            Process process = pb.start();
            activeProcesses.put(jobId, process);

            final String finalJobId = jobId;
            StringBuilder finalOutput = new StringBuilder();
            Thread streamer = new Thread(() -> {
                File logFile = new File(executionDir, finalJobId + ".log");
                System.out.println("[DEBUG] Writing logs to: " + logFile.getAbsolutePath());
                try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                   BufferedWriter fileWriter = new BufferedWriter(new FileWriter(logFile, true))){
                       String line;
                       while ((line = bufferedReader.readLine()) != null) {
//                           parentServer.streamLogToMaster(finalJobId, line);
                           batcher.addLog(line);

                           fileWriter.write(line);
                           fileWriter.newLine();
                           fileWriter.flush();

//                           System.out.println("[STREAM] " + line);

                           synchronized (finalOutput) {
                               finalOutput.append(line).append("\n");
                           }
                       }
               }catch (IOException e) { e.printStackTrace();}
            });

            streamer.start();

            // 4. Wait for completion
            // DAG jobs (long-running training, ETL, ML pipelines) get 24h.
            // Standalone/test jobs keep the 60s safety limit.
            long timeoutSeconds = jobId.startsWith("DAG-") ? 86400 : 60;
            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            streamer.join(); // this ensures we capture the last line of the o/p.

            if (!finished) {
                // destroy() is SIGTERM to the direct child only, so a script that ignores SIGTERM
                // survived its own timeout and kept its slot. Kill the tree, forcibly.
                killTree(process);
                return "ERROR: Script timed out (" + timeoutSeconds + "s limit)";
            }

            // 5. Read Output (Byte-oriented for TitanProtocol compatibility)
//            byte[] outputBytes = process.getInputStream().readAllBytes();
//            String output = new String(outputBytes, StandardCharsets.UTF_8).trim();
            int exitCode = process.exitValue();
            // Format: COMPLETED|ExitCode|OutputContent
            return "COMPLETED|" + exitCode + "|" + finalOutput.toString().trim();

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR: Execution failed - " + e.getMessage();
        } finally {
            activeProcesses.remove(jobId);
            batcher.close();
        }
    }
}
