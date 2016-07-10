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
 * @(#)CommandResult.java   Apr 25, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.io.ByteArrayOutputStream;

/**
 * This is a command result that is returned from an sync execution. The CommandResult will be
 * return from {@link Executor#execute(Command, java.io.File)}  method when forked process will be
 * finished.<br> To get the buffered process stream use {@link #getOut()}.
 *
 * <pre>
 * CommandResult cmdRes = Executor.execute( "startScript.sh", "/usr/bin");
 * System.out.println( cmdRes.getOut() );
 * System.out.println( cmdRes.getCode() );
 * </pre>
 *
 * @author Igor Goldenberg
 * @see Executor
 * @since 1.0
 **/
public class CommandResult {
    private final Command command;
    private final Process process;
    private ByteArrayOutputStream byteOutStream;

    /**
     * Construct command result instance with given arguments
     */
    CommandResult(Process process, Command command) {
        this.process = process;
        this.command = command;
    }

    /**
     * @return the buffered process stream
     * @see Command#getOutputStreamRedirection().
     */
    public String getOut() {
        return byteOutStream != null ? new String(byteOutStream.toByteArray()) : "";
    }

    /**
     * @return the forked process
     */
    public Process getProcess() {
        return process;
    }

    /**
     * @return the execution command
     */
    public Command getCommand() {
        return command;
    }

    /**
     * @return the exit process code
     */
    public int getCode() {
        return process.exitValue();
    }

    void bufferOutputStream() {
        byteOutStream = new ByteArrayOutputStream();
        Executor.redirectOutputStream(process, byteOutStream, command);
    }
}