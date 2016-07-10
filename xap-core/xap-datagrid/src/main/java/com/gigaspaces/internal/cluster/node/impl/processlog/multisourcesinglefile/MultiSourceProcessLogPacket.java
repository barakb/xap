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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author idan
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceProcessLogPacket {
    protected enum MultiSourcePacketState {
        UNCONSUMED, BEING_CONSUMED, CONSUMED
    }

    protected final IReplicationOrderedPacket _packet;
    protected List<MultiSourceProcessLogPacket> _dependencies;
    protected MultiSourcePacketState _state;

    public MultiSourceProcessLogPacket(IReplicationOrderedPacket packet) {
        this._packet = packet;
        this._state = MultiSourcePacketState.UNCONSUMED;
    }

    public void setUnconsumed() {
        _state = MultiSourcePacketState.UNCONSUMED;
    }

    public void setBeingConsumed() {
        _state = MultiSourcePacketState.BEING_CONSUMED;
    }

    public boolean isMultiParticipant() {
        return false;
    }

    public IReplicationPacketData<?> getData() {
        return _packet.getData();
    }

    public boolean isConsolidatedByThisParticipant() {
        return false;
    }

    public boolean isConsumed() {
        return _state == MultiSourcePacketState.CONSUMED;
    }

    public IReplicationOrderedPacket getReplicationPacket() {
        return _packet;
    }

    public long getKey() {
        return _packet.getKey();
    }

    public boolean hasDependencies() {
        if (_dependencies != null) {
            for (MultiSourceProcessLogPacket dependency : _dependencies) {
                if (dependency._state == MultiSourcePacketState.UNCONSUMED)
                    return true;
            }
        }
        return false;
    }

    public void addDependency(MultiSourceProcessLogPacket dependencyPacket) {
        if (_dependencies == null)
            _dependencies = new LinkedList<MultiSourceProcessLogPacket>();
        _dependencies.add(dependencyPacket);
    }

    public boolean isBeingConsumed() {
        return _state == MultiSourcePacketState.BEING_CONSUMED;
    }

    public void setConsumed() {
        _state = MultiSourcePacketState.CONSUMED;
    }

    @Override
    public String toString() {
        ArrayList<Long> keys = new ArrayList<Long>(_dependencies == null ? 0 : _dependencies.size());
        if (_dependencies != null) {
            for (MultiSourceProcessLogPacket dependency : _dependencies)
                keys.add(dependency.getKey());
        }

        return "MultiSourceProcessLogPacket [state=" + _state + ", packet = " + _packet.toString() + ", dependencies=" + keys + "]";
    }

    public boolean isConsumedByOtherParticipant() {
        return false;
    }

    public IReplicationParticipantsMetadata getParticipantsMetadata() {
        throw new UnsupportedOperationException();
    }

    public void setRegisteredInMediator() {
        throw new UnsupportedOperationException();
    }

    public void setData(IReplicationPacketData<?> mergedData) {
        throw new UnsupportedOperationException();
    }

    public boolean isRegisteredInMediator() {
        throw new UnsupportedOperationException();
    }

    public long getCreationTime() {
        throw new UnsupportedOperationException();
    }


}
