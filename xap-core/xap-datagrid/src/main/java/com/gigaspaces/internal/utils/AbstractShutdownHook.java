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

package com.gigaspaces.internal.utils;

import com.gigaspaces.internal.utils.concurrent.GSThread;

import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

/**
 * Base class for jvm shutdown hook implementation.
 *
 * @author Niv Ingberg
 * @since 9.0.1
 */
public abstract class AbstractShutdownHook extends GSThread {
    private static final String LogShutdownHookPath = System.getProperty("com.gigaspaces.shutdownhook.logpath");
    private static final int ShutdownHookTimeout = Integer.getInteger("com.gigaspaces.shutdownhook.timeout", 30);

    public AbstractShutdownHook(String name) {
        super(name);
    }

    @Override
    public void run() {
        FileWriter fileWriter = createFileWriter(LogShutdownHookPath);
        if (fileWriter == null)
            onShutdown();
        else
            onShutdown(fileWriter);
    }

    protected abstract void onShutdown();

    protected void onShutdownTimeout(FileWriter fileWriter, long duration) {
        log(fileWriter, "Timed out (duration=" + duration + "ms).");
    }

    private void onShutdown(FileWriter fileWriter) {
        long time = System.currentTimeMillis();
        log(fileWriter, "Starting...");

        GSThread shutdownThread = new GSThread(new Runnable() {
            @Override
            public void run() {
                onShutdown();
            }
        }, super.getName() + "-async");
        shutdownThread.start();

        for (int i = 0; i < ShutdownHookTimeout; i++) {
            try {
                shutdownThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (!shutdownThread.isAlive())
                break;
            log(fileWriter, "Waiting for shutdown hook to complete (iteration #" + i + ")...");
        }

        time = System.currentTimeMillis() - time;
        if (shutdownThread.isAlive())
            onShutdownTimeout(fileWriter, time);
        else
            log(fileWriter, "Finished (duration=" + time + "ms).");

        closeFileWriter(fileWriter);
    }

    private FileWriter createFileWriter(String path) {
        if (!StringUtils.hasText(path))
            return null;

        path = path + StringUtils.FOLDER_SEPARATOR + getName() + "_" + UUID.randomUUID() + ".log";
        try {
            return new FileWriter(path);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    protected void log(FileWriter fileWriter, String message) {
        if (fileWriter == null)
            return;

        message = StringUtils.getTimeStamp() + " [" + getName() + "] - " + message + StringUtils.NEW_LINE;

        try {
            fileWriter.write(message);
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void closeFileWriter(FileWriter fileWriter) {
        try {
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
