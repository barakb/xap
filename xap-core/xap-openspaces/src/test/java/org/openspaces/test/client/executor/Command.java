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

/*
 * @(#)Command.java   Apr 25, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.io.OutputStream;

/**
 * This interface defines an executable command (CLI) that is run by the Executor.
 *
 * @author Igor Goldenberg
 * @see Executor
 * @since 1.0
 **/
public interface Command {
    /**
     * @return an array of command line arguments, if any, that the command requires for execution.
     */
    public Argument[] getArguments();

    /**
     * Obtain the command and arguments as were passed to Runtime.exec(String[], String[],
     * java.io.File)
     */
    public String[] getCommand();

    /**
     * The java.io.OutputStream to redirect the processes output to.
     *
     * @return The output stream to redirect to, or <code>null</code> if no redirection is required.
     */
    public OutputStream getOutputStreamRedirection();

    /**
     * invokes by {@link Executor} before command execution (before fork process)
     */
    public void beforeExecute() throws ExecutionException;

    /**
     * invokes by {@link Executor} after command execution (after fork process).
     *
     * @param process The forked process.
     */
    public void afterExecute(Process process) throws ExecutionException;

    /**
     * invokes by {@link Executor} after {@link AsyncCommandResult#stop(boolean)} destroy process.
     *
     * @param force <code>true</code> if command process should be forcibly destroyed.
     **/
    public void destroy(boolean force);
}