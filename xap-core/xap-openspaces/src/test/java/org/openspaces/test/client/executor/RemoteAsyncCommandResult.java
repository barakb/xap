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
 * @(#)RemoteAsyncCommandResult.java   Jul 22, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This is a command result that is returned from an {@link Executor#executeAsync(RemoteJavaCommand,
 * java.io.File)} async execution. <br> The result stores the state of the execution and also allows
 * clients to work with forked JVM as remote service via user defined remote interface. <br> {@link
 * #getRemoteStub()} returns stub to remote java process and allows perform RMI calls on forked jvm.
 * <br> {@link #getProcessAdmin()} allows to get admin object of forked JVM.
 *
 * @author Igor Goldenberg
 * @see #getRemoteStub()
 * @see #getProcessAdmin()
 * @see RemoteJavaCommand
 * @since 1.0
 **/
public class RemoteAsyncCommandResult<R extends Remote>
        extends AsyncCommandResult {
    private final RemoteJavaCommand<R> remoteCmd;

    /**
     * Constructor.
     */
    RemoteAsyncCommandResult(Process proc, RemoteJavaCommand<R> cmd) {
        super(proc, cmd);

        remoteCmd = cmd;
    }

    /**
     * @return a remote stub to remote java process to perform RMI calls on forked JVM
     */
    public R getRemoteStub() {
        return remoteCmd.getRemoteStub();
    }

    /**
     * @return an admin object of forked JVM
     */
    public IRemoteJavaProcessAdmin getProcessAdmin() {
        return remoteCmd.getProcessAdmin();
    }

    /**
     * @return RemoteJavaCommand this {@link RemoteAsyncCommandResult} belongs to.
     */
    @Override
    public RemoteJavaCommand<R> getCommand() {
        return remoteCmd;
    }

    /**
     * force killing forkable VM by direct remote call on remote JVM
     */
    @Override
    public void stop(boolean force) {
        /* destroy process, be aware the process may not destroyed right away. */
        try {
            if (force) {
                getProcessAdmin().killVM();
            }
        } catch (RemoteException ex) {
            /* don't care */
            ProcessLogger.log("Exception caught while killing VM", ex);
        }

        /* destroy process and unregister from managed registry */
        super.stop(force);
    }
}