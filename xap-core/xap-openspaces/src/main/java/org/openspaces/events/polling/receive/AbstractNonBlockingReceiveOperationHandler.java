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


package org.openspaces.events.polling.receive;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.SpaceInterruptedException;
import org.springframework.dao.DataAccessException;

/**
 * Support class to perform either non blocking or blocking receive operation.
 *
 * @author kimchy
 */
public abstract class AbstractNonBlockingReceiveOperationHandler implements ReceiveOperationHandler {

    protected boolean nonBlocking = false;

    protected int nonBlockingFactor = 10;

    /**
     * Allows to configure the take operation to be performed in a non blocking manner.
     *
     * <p>If configured to use non blocking, will perform {@link #setNonBlockingFactor(int)} number
     * of non blocking operations (default to 10) within the receive timeout.
     */
    public void setNonBlocking(boolean nonBlocking) {
        this.nonBlocking = nonBlocking;
    }

    /**
     * The factor (number of times) to perform the receive operation when working in non blocking
     * mode within the given receive timeout. Defaults to <code>10</code>.
     */
    public void setNonBlockingFactor(int nonBlockingFactor) {
        this.nonBlockingFactor = nonBlockingFactor;
    }

    /**
     * Performs the receive operation. If blocking, we call {@link #doReceiveBlocking(Object,
     * org.openspaces.core.GigaSpace, long)} and expect it to block for the receive timeout (or
     * until a match is found). If non blocking, will perform {@link #setNonBlockingFactor(int)}
     * number of {@link #doReceiveNonBlocking(Object, org.openspaces.core.GigaSpace)} operations
     * within the receive timeout (default factor is 10).
     */
    public Object receive(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        if (!nonBlocking) {
            return doReceiveBlocking(template, gigaSpace, receiveTimeout);
        }
        long sleepTime = receiveTimeout / nonBlockingFactor;
        for (int i = 0; i < nonBlockingFactor; i++) {
            Object event = doReceiveNonBlocking(template, gigaSpace);
            if (event != null) {
                return event;
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new SpaceInterruptedException("Interrupted while performing non blocking receive operation");
            }
        }
        return null;
    }

    /**
     * Performs a receive operations in a blocking manner.
     */
    protected abstract Object doReceiveBlocking(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException;

    /**
     * Performs a receive operations in a non blocking manner.
     */
    protected abstract Object doReceiveNonBlocking(Object template, GigaSpace gigaSpace) throws DataAccessException;
}
