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

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationParticipantsMetadata;
import com.gigaspaces.time.SystemTime;

/**
 * @author idan
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceProcessLogMultiParticipantPacket
        extends MultiSourceProcessLogPacket {
    private final IReplicationParticipantsMediator _mediator;
    private IReplicationPacketData<?> _mergedData;
    private IReplicationParticipantsMetadata _participantsMetadata;
    private boolean _registeredInMediator;
    private long _creationTime;

    public MultiSourceProcessLogMultiParticipantPacket(
            IReplicationOrderedPacket packet,
            IReplicationParticipantsMediator participantsMediator,
            IReplicationParticipantsMetadata participantsMetadata) {
        super(packet);
        this._mediator = participantsMediator;
        this._participantsMetadata = participantsMetadata;
        this._creationTime = SystemTime.timeMillis();
    }

    @Override
    public void setData(IReplicationPacketData<?> mergedData) {
        _mergedData = mergedData;
    }

    @Override
    public IReplicationPacketData<?> getData() {
        return _mergedData != null ? _mergedData : _packet.getData();
    }

    @Override
    public boolean hasDependencies() {
        if (_dependencies != null) {
            for (MultiSourceProcessLogPacket dependency : _dependencies) {
                if (isConsolidatedByThisParticipant()) {
                    if (dependency._state == MultiSourcePacketState.UNCONSUMED)
                        return true;
                } else {
                    if (dependency._state != MultiSourcePacketState.CONSUMED)
                        return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isConsolidatedByThisParticipant() {
        return _mergedData != null;
    }

    @Override
    public boolean isMultiParticipant() {
        return true;
    }

    @Override
    public boolean isRegisteredInMediator() {
        return _registeredInMediator;
    }

    @Override
    public void setRegisteredInMediator() {
        _registeredInMediator = true;
    }

    @Override
    public IReplicationParticipantsMetadata getParticipantsMetadata() {
        return _participantsMetadata;
    }

    @Override
    public long getCreationTime() {
        return _creationTime;
    }

    @Override
    public boolean isConsumedByOtherParticipant() {
        return !isConsolidatedByThisParticipant()
                && _mediator.isConsumed(_participantsMetadata.getContextId());
    }
}
