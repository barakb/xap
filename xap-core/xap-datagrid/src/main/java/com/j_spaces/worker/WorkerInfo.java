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
 * @(#)WorkerInfo.java 1.0  30.08.2004  15:36:13
 */

package com.j_spaces.worker;

/**
 * Full information about worker.
 *
 * @author Igor Goldenberg
 * @version 4.0
 * @deprecated
 **/
@Deprecated
@com.gigaspaces.api.InternalApi
public class WorkerInfo {
    private String workerName;
    private String className;
    private String arg;
    private String description;
    private IWorker workerImpl;
    private Thread workerThread;
    private boolean startIfPrimaryOnly;
    private boolean shutdownSpaceOnInitFailure;

    /**
     * Constructor.
     *
     * @param workerImpl   The IWorker implementation.
     * @param workerThread The Worker Thread.
     * @param workerName   Worker name.
     * @param className    Worker class name.
     * @param arg          Worker arguments.
     * @param description  Worker description.
     **/
    public WorkerInfo(IWorker workerImpl, Thread workerThread, String workerName, String className, String arg,
                      String description, boolean startIfPrimaryOnly, boolean shutdownSpaceOnInitFailure) {
        this.workerImpl = workerImpl;
        this.workerThread = workerThread;
        this.workerName = workerName;
        this.className = className;
        this.arg = arg;
        this.description = description;
        this.startIfPrimaryOnly = startIfPrimaryOnly;
        this.shutdownSpaceOnInitFailure = shutdownSpaceOnInitFailure;
    }

    /**
     * @return Returns the arg.
     **/
    public String getArg() {
        return arg;
    }

    /**
     * @return Returns the className.
     **/
    public String getClassName() {
        return className;
    }

    /**
     * @return Returns the description.
     **/
    public String getDescription() {
        return description;
    }

    /**
     * @return Returns the workerName.
     **/
    public String getWorkerName() {
        return workerName;
    }

    /**
     * @return Returns the IWorker implementation.
     **/
    public IWorker getWorker() {
        return workerImpl;
    }


    /**
     * @return Returns the Thread of IWorker implementation. This method called by WorkerManager.
     **/
    protected Thread getWorkerThread() {
        return workerThread;
    }

    /**
     * @return startIfPrimaryOnly
     */
    public boolean isStartIfPrimaryOnly() {
        return startIfPrimaryOnly;
    }

    /**
     * @return shutdownSpaceOnInitFailure
     */
    public boolean isShutdownSpaceOnInitFailure() {
        return shutdownSpaceOnInitFailure;
    }


}
