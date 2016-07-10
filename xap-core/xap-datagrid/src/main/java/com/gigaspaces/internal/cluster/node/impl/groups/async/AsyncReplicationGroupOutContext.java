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

package com.gigaspaces.internal.cluster.node.impl.groups.async;

import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.utils.Textualizer;

@com.gigaspaces.api.InternalApi
public class AsyncReplicationGroupOutContext extends ReplicationGroupOutContext implements IAsyncReplicationGroupOutContext {

    // Not volatile, context should not be passed between threads
    private int _packetCount;

    public AsyncReplicationGroupOutContext(String groupName) {
        super(groupName);
    }

    public void addOrderedPacket(IReplicationOrderedPacket packet) {
        _packetCount++;
        super.afterAddingOrderedPacket(packet);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        return _packetCount;
    }

    public void clear() {
        _packetCount = 0;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("packetCount", size());
    }

}
