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
 * @(#)IRemoteJavaProcessAdmin.java   Jul 22, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.rmi.RemoteException;

/**
 * This interface provides admin functionality of forkable java process {@link RemoteJavaCommand}.
 *
 * @author Igor Goldenberg
 * @see Executor#executeAsync(RemoteJavaCommand, java.io.File)
 * @see RemoteAsyncCommandResult#getProcessAdmin()
 * @see RemoteJavaCommand
 * @since 1.0
 **/
public interface IRemoteJavaProcessAdmin
        extends IJavaProcess {
    /**
     * Checks by remote method call whether forked java process is alive.
     *
     * @return <code>true</code> if forked java process is alive.
     * @throws RemoteException      Connection lost with java process, the process might be already
     *                              destroyed.
     * @throws ForkProcessException Failed to fork java process.
     **/
    public boolean isAlive() throws RemoteException, ForkProcessException;

    /**
     * Force to kill java process.
     *
     * @throws RemoteException Connection lost with java process, the process might be already
     *                         destroyed.
     */
    public void killVM() throws RemoteException;

    /**
     * Creates a dump of the process JVM.
     */
    public void dumpVM() throws RemoteException;
}