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


package org.openspaces.core.space.support;

import com.j_spaces.worker.IWorker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.space.mode.AfterSpaceModeChangeEvent;
import org.openspaces.core.space.mode.BeforeSpaceModeChangeEvent;
import org.openspaces.core.util.SpaceUtils;
import org.openspaces.events.EventDriven;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.util.Assert;

/**
 * An adapter class that can run legacy {@link com.j_spaces.worker.IWorker} implementations as beans
 * providing the same behavior when configured within a Space.
 *
 * <p>Note, the adapter aims to simplify the migration from "worker" based applications to the new
 * processing unit architecture.
 *
 * @author kimchy
 * @deprecated use {@link EventDriven} containers instead
 */
@Deprecated
public class WorkerAdapter implements InitializingBean, DisposableBean, ApplicationListener {

    private static final Log logger = LogFactory.getLog(WorkerAdapter.class);

    private IWorker worker;

    private Thread thread;

    private String argument;

    private String workerName;

    private GigaSpace gigaSpace;

    private boolean activeWhenBackup = false;

    public WorkerAdapter() {
    }

    /**
     * Provides the worker implementation that will be run.
     */
    public void setWorker(IWorker worker) {
        this.worker = worker;
    }

    /**
     * Sets the {@link org.openspaces.core.GigaSpace} that will be used to pass an {@link
     * com.j_spaces.core.IJSpace} instance to the worker init method.
     */
    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    /**
     * Provides an optional argument that will be passed to {@link com.j_spaces.worker.IWorker#init(com.j_spaces.core.IJSpace,
     * String, String)} (the last argument).
     */
    public void setArgument(String argument) {
        this.argument = argument;
    }

    /**
     * Sets the worker name that will be used as the second parameter to the worker init method.
     */
    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    /**
     * Should the worker be active only when the Space is in primary mode. Defaults to
     * <code>false</code>. Setting this to <code>true</code> means that the worker will start
     * regardless of the space state.
     */
    public void setActiveWhenBackup(boolean activeWhenBackup) {
        this.activeWhenBackup = activeWhenBackup;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(worker, "worker is required");
        Assert.notNull(worker, "gigaSpace is required");
        if (activeWhenBackup) {
            startWorker();
        }
    }

    public void destroy() throws Exception {
        stopWorker();
    }

    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (!activeWhenBackup) {
            if (applicationEvent instanceof AfterSpaceModeChangeEvent) {
                AfterSpaceModeChangeEvent spEvent = (AfterSpaceModeChangeEvent) applicationEvent;
                if (spEvent.isPrimary()) {
                    if (SpaceUtils.isSameSpace(spEvent.getSpace(), gigaSpace.getSpace())) {
                        try {
                            startWorker();
                        } catch (Exception e) {
                            logger.error("Failed to start worker [" + workerName + "]", e);
                        }
                    }
                }
            } else if (applicationEvent instanceof BeforeSpaceModeChangeEvent) {
                BeforeSpaceModeChangeEvent spEvent = (BeforeSpaceModeChangeEvent) applicationEvent;
                if (!spEvent.isPrimary()) {
                    if (SpaceUtils.isSameSpace(spEvent.getSpace(), gigaSpace.getSpace())) {
                        stopWorker();
                    }
                }
            }
        }
    }

    private void startWorker() throws Exception {
        if (thread == null) {
            worker.init(gigaSpace.getSpace(), workerName, argument);
            thread = new Thread(worker);
            thread.setName(workerName + gigaSpace.getSpace().getName());
            thread.start();
        }
    }

    private void stopWorker() {
        if (thread == null) {
            return;
        }
        try {
            worker.close();
        } finally {
            thread = null;
        }
    }
}
