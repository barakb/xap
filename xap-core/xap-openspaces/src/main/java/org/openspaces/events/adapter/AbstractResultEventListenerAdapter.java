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

import com.j_spaces.core.client.UpdateModifiers;

import org.openspaces.core.GigaSpace;
import org.openspaces.events.SpaceDataEventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionStatus;

/**
 * A base class event listener allowing for event listeners result handling by writing it back to
 * the space. Subclasses should implement {@link #onEventWithResult(Object, GigaSpace,
 * TransactionStatus, Object)} with the result being writing to back to the space. The write lease
 * can be controlled using {@link #setWriteLease(long)}.
 *
 * @author kimchy
 */
public abstract class AbstractResultEventListenerAdapter implements SpaceDataEventListener {

    private long writeLease = Long.MAX_VALUE;

    private boolean updateOrWrite = true;

    private long updateTimeout = 0;

    /**
     * The lease time the result will be written under (in milliseconds). Defaults to {@link
     * net.jini.core.lease.Lease#FOREVER}.
     *
     * @param writeLease The lease time the result will be written under
     */
    public void setWriteLease(long writeLease) {
        this.writeLease = writeLease;
    }

    /**
     * Sets if the write operation will perform an update in case the entry result already exists in
     * the space. Default to <code>true</code>.
     */
    public void setUpdateOrWrite(boolean updateOrWrite) {
        this.updateOrWrite = updateOrWrite;
    }

    /**
     * Sets the update timeout (im milliseconds) in case the flag {@link #setUpdateOrWrite(boolean)}
     * is set to <code>true</code>.
     */
    public void setUpdateTimeout(long updateTimeout) {
        this.updateTimeout = updateTimeout;
    }

    /**
     * Implements the {@link org.openspaces.events.SpaceDataEventListener} by delegating to {@link
     * #onEventWithResult(Object, org.openspaces.core.GigaSpace, org.springframework.transaction.TransactionStatus,
     * Object)} and writing the result back to the space (if it is not <code>null</code>) using
     * {@link #handleResult(Object, org.openspaces.core.GigaSpace)}.
     */
    public void onEvent(Object data, GigaSpace gigaSpace, TransactionStatus txStatus, Object source) {
        Object result = onEventWithResult(data, gigaSpace, txStatus, source);
        handleResult(result, gigaSpace);
    }

    /**
     * Writes the result back to the space (if not <code>null</code>) under the configured write
     * lease. Allows to be overridden in order to implement more advance result handling.
     *
     * <p> By default handles both single object and array of objects. Takes into account the {@link
     * #setUpdateOrWrite(boolean) 'updateOrWrite'} flag when writing/updating the result back to the
     * space.
     *
     * @param result    The result to write back to the space
     * @param gigaSpace The GigaSpace instance to operate against the space
     */
    protected void handleResult(Object result, GigaSpace gigaSpace) throws DataAccessException {
        if (result != null) {
            if (result instanceof Object[]) {
                Object[] resultArr = (Object[]) result;
                if (updateOrWrite)
                    gigaSpace.writeMultiple(resultArr, writeLease, UpdateModifiers.UPDATE_OR_WRITE | UpdateModifiers.NO_RETURN_VALUE);
                else
                    gigaSpace.writeMultiple(resultArr, writeLease, UpdateModifiers.WRITE_ONLY | UpdateModifiers.NO_RETURN_VALUE);
            } else {
                if (updateOrWrite) {
                    gigaSpace.write(result, writeLease, updateTimeout, UpdateModifiers.UPDATE_OR_WRITE | UpdateModifiers.NO_RETURN_VALUE);
                } else {
                    gigaSpace.write(result, writeLease, updateTimeout, UpdateModifiers.WRITE_ONLY | UpdateModifiers.NO_RETURN_VALUE);
                }
            }
        }
    }

    /**
     * An event listener callback allowing to return a result that will be written back to the
     * space.
     *
     * @param data      The event data object
     * @param gigaSpace A GigaSpace instance that can be used to perform additional operations
     *                  against the space
     * @param txStatus  An optional transaction status allowing to rollback a transaction
     *                  programmatically
     * @param source    Optional additional data or the actual source event data object (where
     *                  relevant)
     * @return A result object that will be written back to the space
     */
    protected abstract Object onEventWithResult(Object data, GigaSpace gigaSpace, TransactionStatus txStatus,
                                                Object source);
}
