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

/**
 * Responsible for synchronizing replication process logs multiple participants operations in a
 * reliable async target.
 *
 * @author idan
 * @since 8.0.4
 */
public interface IReplicationParticipantsMediator {

    /**
     * Gets all data for a multiple participants replication operation. If not all data arrived the
     * return value is null.
     *
     * @param packet The packet to be added to the context in case not all packets arrived.
     * @return All data if all data arrived, otherwise null.
     */
    IReplicationPacketData<?>[] getAllParticipantsData(
            MultiSourceProcessLogPacket packet);

    /**
     * @return true if the provided contextId associated operation has been consumed.
     */
    boolean isConsumed(Object contextId);

    /**
     * Sets the provided contextId as consumed.
     */
    void setConsumed(IReplicationParticipantsMetadata participantsMetadata);

    /**
     * Removes multiple participant context from mediator according to the provided meta data.
     */
    void remove(IReplicationParticipantsMetadata participantsMetadata);

    /**
     * Attempts to abort consolidation for the provided meta data.
     *
     * @return true if aborted successfully, false if already consolidated.
     */
    boolean abortConsolidation(
            IReplicationParticipantsMetadata participantsMetadata);

}
