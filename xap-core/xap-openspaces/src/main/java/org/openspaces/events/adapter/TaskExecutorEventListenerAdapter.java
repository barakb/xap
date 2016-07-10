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

package org.openspaces.events.adapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.events.SpaceDataEventListener;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.TransactionStatus;
import org.springframework.util.Assert;

/**
 * An adapter that delegates the execution of a {@link org.openspaces.events.SpaceDataEventListener}
 * to Spring {@link TaskExecutor} implementation (usually to be executed in a different thread).
 *
 * <p>Very handy when using notifications in order to release the notification thread.
 *
 * <p>Defualt task executor uses {@link SimpleAsyncTaskExecutor} which creates a new thread for each
 * request.
 *
 * @author kimchy
 * @see org.springframework.core.task.TaskExecutor
 */
public class TaskExecutorEventListenerAdapter implements SpaceDataEventListener, InitializingBean, EventListenerAdapter {

    private static Log logger = LogFactory.getLog(TaskExecutorEventListenerAdapter.class);

    private TaskExecutor taskExecutor;

    private SpaceDataEventListener delegate;

    /**
     * Sets the task executor to be used to delegate the execution of the {@link
     * #setDelegate(org.openspaces.events.SpaceDataEventListener) delegate} listener.
     *
     * <p>Defualts to {@link SimpleAsyncTaskExecutor} which creates a new thread for each request.
     */
    public void setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * Sets the delegate that will invoked using the task executor.
     */
    public void setDelegate(SpaceDataEventListener delegate) {
        this.delegate = delegate;
    }

    /**
     * Initializes the task executor adapter. Expects the delegate to be set. If no {@link
     * #setTaskExecutor(org.springframework.core.task.TaskExecutor) taskExecutor} is provided will
     * create a default one using {@link org.springframework.core.task.SimpleAsyncTaskExecutor}.
     */
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(delegate, "delegate SpaceDataEventListener must not be null");
        if (taskExecutor == null) {
            SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
            simpleAsyncTaskExecutor.setDaemon(true);
            taskExecutor = simpleAsyncTaskExecutor;
        }
    }

    public Object getActualEventListener() {
        return this.delegate;
    }

    /**
     * Listens for events and and delegates them to the {@link #setDelegate(org.openspaces.events.SpaceDataEventListener)}
     * to be executed using the provided {@link #setTaskExecutor(org.springframework.core.task.TaskExecutor)}.
     */
    public void onEvent(final Object data, final GigaSpace gigaSpace, final TransactionStatus txStatus, final Object source) {
        taskExecutor.execute(new Runnable() {
            public void run() {
                try {
                    delegate.onEvent(data, gigaSpace, txStatus, source);
                } catch (Exception e) {
                    logger.warn("Delegate threw an exception within a separate thread, ignoring...", e);
                }
            }
        });
    }
}
