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

package org.openspaces.archive;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;

import org.openspaces.core.GigaSpace;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.events.polling.SimplePollingEventListenerContainer;
import org.openspaces.events.polling.receive.MultiTakeReceiveOperationHandler;
import org.openspaces.events.polling.receive.SingleTakeReceiveOperationHandler;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.TransactionStatus;

/**
 * Takes objects specified in the template into the archive handler defined by {@link
 * #setArchiveHandler(ArchiveOperationHandler)} This container can be used to take (remove) objects
 * from the Space and persist them into an external service.
 *
 * @author Itai Frenkel
 * @since 9.1.1
 */
public class ArchivePollingContainer
        extends SimplePollingEventListenerContainer
        implements SpaceDataEventListener<Object> {

    private ArchiveOperationHandler archiveHandler;
    private int batchSize = 50; // == MultiTakeReceiveOperationHandler#DEFAULT_MAX_ENTRIES;
    private long nonBlockingSleep = 100;
    private boolean useFifoGrouping = false;

    public ArchivePollingContainer() {
        super.setEventListener(this);
    }

    @Required
    public void setArchiveHandler(ArchiveOperationHandler archiveHandler) {
        this.archiveHandler = archiveHandler;
    }


    @Override
    protected void validateConfiguration() {
        super.validateConfiguration();
        if (archiveHandler == null) {
            throw new IllegalStateException("Archive handler cannot be null");
        }
    }

    @Override
    public void initialize() {

        ISpaceProxy space = (ISpaceProxy) getGigaSpace().getSpace();
        boolean clustered = space.isClustered();
        if (archiveHandler.supportsBatchArchiving()) {
            MultiTakeReceiveOperationHandler receiveHandler = new MultiTakeReceiveOperationHandler();
            receiveHandler.setMaxEntries(batchSize);
            if (clustered) {
                //remote clustered proxy does not support blocking takeMultiple
                receiveHandler.setNonBlocking(true);
                receiveHandler.setNonBlockingFactor(calcNonBlockingFactor());
            }
            receiveHandler.setUseFifoGrouping(isUseFifoGrouping());
            super.setReceiveOperationHandler(receiveHandler);
            super.setPassArrayAsIs(true);
        } else {
            SingleTakeReceiveOperationHandler receiveHandler = new SingleTakeReceiveOperationHandler();
            if (clustered) {
                //remote clustered proxy does not support blocking take
                receiveHandler.setNonBlocking(true);
                receiveHandler.setNonBlockingFactor(calcNonBlockingFactor());
                receiveHandler.setUseFifoGrouping(isUseFifoGrouping());
            }
            super.setReceiveOperationHandler(receiveHandler);
        }

        if (getExceptionHandler() == null) {
            setExceptionHandler(new DefaultArchivePollingContainerExceptionHandler());
        }

        super.initialize();
    }

    private int calcNonBlockingFactor() {
        long nonblockingFactor = getReceiveTimeout() / getNonBlockingSleep();
        return (int) Math.max(1, nonblockingFactor);
    }

    @Override
    public void onEvent(Object data, GigaSpace gigaSpace, TransactionStatus txStatus, Object source) {
        if (isPassArrayAsIs()) {
            archiveHandler.archive((Object[]) data);
        } else {
            archiveHandler.archive(data);
        }
    }

    /**
     * @param batchSize - The maximum number of objects to hand over to the archiver in one method
     *                  call. This parameter has affect only if the archive handler {@link
     *                  ArchiveOperationHandler#supportsBatchArchiving()}
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public long getNonBlockingSleep() {
        return nonBlockingSleep;
    }

    /**
     * In case the space is a proxy to a remote clustered space we use non-blocking take operation
     * for polling. If the take returned without any object the thread sleeps for 100 milliseconds.
     * Use this method to set the sleep timeout to a different value.
     *
     * @param nonBlockingSleepMilliseconds - the time to sleep if take returned no values
     *                                     (milliseconds)
     */
    public void setNonBlockingSleep(long nonBlockingSleepMilliseconds) {
        this.nonBlockingSleep = nonBlockingSleepMilliseconds;
    }

    public boolean isUseFifoGrouping() {
        return useFifoGrouping;
    }

    /**
     * Enables take operations that are performed with FIFO Grouping enabled
     */
    public void setUseFifoGrouping(boolean useFifoGrouping) {
        this.useFifoGrouping = useFifoGrouping;
    }
}