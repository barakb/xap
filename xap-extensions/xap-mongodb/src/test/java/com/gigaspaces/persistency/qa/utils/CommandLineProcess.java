/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.persistency.qa.utils;

import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandLineProcess implements Runnable {

    private final List<String> command;
    private int exitValue;
    Process process;
    private final Map<String, String> env = new HashMap<String, String>();
    private final String workingDir;

    public CommandLineProcess(List<String> cmd, String workingDir) {

        if (cmd == null || cmd.isEmpty())
            throw new IllegalArgumentException("cmd");

        this.command = cmd;
        this.workingDir = workingDir;
    }

    public void addEnvironmentVariable(String key, String value) {
        env.put(key, value);
    }

    public void run() {
        execute(command);

        this.exitValue = process.exitValue();
    }

    private void execute(List<String> command2) {
        try {
            ProcessBuilder builder = new ProcessBuilder(command2);

            if (env.size() > 0)
                builder.environment().putAll(env);

            if (StringUtils.hasLength(workingDir))
                builder.directory(new File(workingDir));

            builder.redirectErrorStream(true);
            process = builder.start();

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(
                    process.getInputStream()));
            try {
                String line;

                while ((line = stdInput.readLine()) != null) {
                    System.out.println(line);
                }

                process.waitFor();
            } finally {
                stdInput.close();
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    public void stop() {
        if (process != null)
            process.destroy();
    }

    public int getExitValue() {
        return exitValue;
    }
}
