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

package com.gigaspaces.internal.cluster.node.impl.groups.sync;

import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.utils.Textualizer;

import java.util.LinkedList;
import java.util.List;


@com.gigaspaces.api.InternalApi
public class SyncReplicationGroupOutContext extends ReplicationGroupOutContext implements ISyncReplicationGroupOutContext {

    // Not volatile, context should not be passed between threads
    private List<IReplicationOrderedPacket> _orderedPackets;
    private IReplicationOrderedPacket _singlePacket;

    public SyncReplicationGroupOutContext(String name) {
        super(name);
    }

    public void addOrderedPacket(IReplicationOrderedPacket packet) {
        if (_singlePacket == null)
            _singlePacket = packet;
        else if (_orderedPackets != null)
            _orderedPackets.add(packet);
        else {
            _orderedPackets = new LinkedList<IReplicationOrderedPacket>();
            _orderedPackets.add(_singlePacket);
            _orderedPackets.add(packet);
        }
        super.afterAddingOrderedPacket(packet);

    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.cluster.node.impl.groups.sync.ISyncReplicationGroupOutContext#getOrderedPackets()
     */
    public List<IReplicationOrderedPacket> getOrderedPackets() {
        if (isSinglePacket())
            throw new IllegalStateException();
        return _orderedPackets;
    }

    public boolean isSinglePacket() {
        return !isEmpty() && _orderedPackets == null;
    }

    public boolean isEmpty() {
        return _singlePacket == null && _orderedPackets == null;
    }

    public IReplicationOrderedPacket getSinglePacket() {
        if (_orderedPackets != null)
            throw new IllegalStateException();
        return _singlePacket;
    }

    public int size() {
        if (isEmpty())
            return 0;
        return isSinglePacket() ? 1 : _orderedPackets.size();
    }

    public void clear() {
        _singlePacket = null;
        _orderedPackets = null;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        if (isSinglePacket())
            textualizer.append("packet", _singlePacket);
        else
            textualizer.append("packets", _orderedPackets);
    }

}
