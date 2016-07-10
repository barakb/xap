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

package com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationParticipantsMetadata;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerObjectMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class ReplicationParticipantsMediator
        implements IReplicationParticipantsMediator {
    private enum ConsolidationState {
        /**
         * Waiting for all participants packets to arrive.
         */
        WAITING_FOR_PARTICIPANTS,
        /**
         * Consolidation is aborted - each participant packet will be individually executed.
         */
        ABORTED,
        /**
         * Packets were consolidated but not yet consumed.
         */
        CONSOLIDATED,
        /**
         * Multi participant operation is consolidated and consumed.
         */
        CONSUMED
    }

    private final Map<Object, ReplicationParticipantsData> _contextData = new HashMap<Object, ReplicationParticipantsData>();
    private final ReentrantLock _lock = new ReentrantLock();

    @Override
    public IReplicationPacketData<?>[] getAllParticipantsData(
            MultiSourceProcessLogPacket packet) {
        final IReplicationParticipantsMetadata participantsMetadata = packet.getParticipantsMetadata();
        if (participantsMetadata.getTransactionParticipantsCount() == 1) {
            return new IReplicationPacketData<?>[]{packet.getData()};
        }

        _lock.lock();

        try {
            if (_contextData.containsKey(participantsMetadata.getContextId())) {
                final ReplicationParticipantsData participantsData = _contextData.get(participantsMetadata.getContextId());
                // Call the underlying replication packet getData() - the direct one might somehow return an already merged data
                participantsData.addParticipantData(packet.getReplicationPacket().getData(), participantsMetadata);

                // If all participants data arrived - return all data
                if (participantsData.shouldBeConsolidatedByThisParticipant(participantsMetadata)) {
                    participantsData.setConsolidationState(ConsolidationState.CONSOLIDATED);
                    return participantsData.getParticipantsData();
                }
            } else {
                _contextData.put(participantsMetadata.getContextId(), new ReplicationParticipantsData(packet.getReplicationPacket().getData(), participantsMetadata));
            }
        } finally {
            _lock.unlock();
        }
        return null;
    }

    @Override
    public boolean isConsumed(Object contextId) {
        _lock.lock();
        try {
            final ReplicationParticipantsData data = getReplicationParticipantsData(contextId);
            return data != null ? data.isConsumed() : false;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void setConsumed(IReplicationParticipantsMetadata participantsMetadata) {
        _lock.lock();
        try {
            final ReplicationParticipantsData data = getReplicationParticipantsData(participantsMetadata.getContextId());
            if (data == null)
                throw new IllegalStateException("Attempted to get participants data for an unknown context id: " + participantsMetadata.getContextId());
            data.setConsumed();
            data.remove(participantsMetadata);
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void remove(IReplicationParticipantsMetadata participantsMetadata) {
        _lock.lock();
        try {
            final ReplicationParticipantsData data = getReplicationParticipantsData(participantsMetadata.getContextId());
            // If all data has been removed, remove multiple participant context from Mediator
            // data can be null if a pending part timed out and executed solely after its corresponding parts have been executed
            if (data != null && data.remove(participantsMetadata))
                _contextData.remove(participantsMetadata.getContextId());
        } finally {
            _lock.unlock();
        }
    }

    /**
     * @return true if successfully aborted, otherwise false.
     */
    @Override
    public boolean abortConsolidation(
            IReplicationParticipantsMetadata participantsMetadata) {
        _lock.lock();
        try {
            final ReplicationParticipantsData data = getReplicationParticipantsData(participantsMetadata.getContextId());
            if (data == null)
                throw new IllegalStateException("Attempted to get participants data for an unknown context id: " + participantsMetadata.getContextId());

            if (data.getConsolidationState() == ConsolidationState.WAITING_FOR_PARTICIPANTS || data.getConsolidationState() == ConsolidationState.ABORTED) {
                data.setConsolidationState(ConsolidationState.ABORTED);
                return true;
            }
            return false;
        } finally {
            _lock.unlock();
        }
    }

    private ReplicationParticipantsData getReplicationParticipantsData(
            Object contextId) {
        return _contextData.get(contextId);
    }

    /**
     * Holds information of a multiple participants replication operation.
     *
     * @author idan
     * @since 8.0.4
     */
    private class ReplicationParticipantsData {
        private final IntegerObjectMap<IReplicationPacketData<?>> _participantsData = CollectionsFactory.getInstance().createIntegerObjectMap();
        private ConsolidationState _consolidationState = ConsolidationState.WAITING_FOR_PARTICIPANTS;
        private int _consolidatorParticipantId = -1;

        public ReplicationParticipantsData(IReplicationPacketData<?> data, IReplicationParticipantsMetadata participantsMetadata) {
            addParticipantData(data, participantsMetadata);
        }

        public void setConsolidationState(ConsolidationState consolidationState) {
            _consolidationState = consolidationState;
        }

        public ConsolidationState getConsolidationState() {
            return _consolidationState;
        }

        public boolean shouldBeConsolidatedByThisParticipant(
                IReplicationParticipantsMetadata participantsMetadata) {
            if (_consolidationState == ConsolidationState.ABORTED
                    || _participantsData.size() != participantsMetadata.getTransactionParticipantsCount())
                return false;
            if (_consolidatorParticipantId == -1) {
                _consolidatorParticipantId = participantsMetadata.getParticipantId();
                return true;
            }
            return _consolidatorParticipantId == participantsMetadata.getParticipantId();
        }

        /**
         * @return true if all data has been removed.
         */
        public boolean remove(IReplicationParticipantsMetadata participantsMetadata) {
            _participantsData.remove(participantsMetadata.getParticipantId());
            return _participantsData.isEmpty();
        }

        public boolean isConsumed() {
            return _consolidationState == ConsolidationState.CONSUMED;
        }

        public void setConsumed() {
            _consolidationState = ConsolidationState.CONSUMED;
        }

        public void addParticipantData(IReplicationPacketData<?> data, IReplicationParticipantsMetadata participantsMetadata) {
            _participantsData.put(participantsMetadata.getParticipantId(), data);
        }

        public IReplicationPacketData<?>[] getParticipantsData() {
            return _participantsData.getValues(new IReplicationPacketData<?>[_participantsData.size()]);
        }

    }


}
