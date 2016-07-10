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

package com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.StringUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class ReliableAsyncHandshakeIteration
        implements IHandshakeIteration {
    private static final long serialVersionUID = 1L;
    private List<IReplicationOrderedPacket> _packets;

    public ReliableAsyncHandshakeIteration() {
    }

    public ReliableAsyncHandshakeIteration(List<IReplicationOrderedPacket> packets) {
        _packets = packets;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _packets = IOUtils.readObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _packets);
    }

    public List<IReplicationOrderedPacket> getPackets() {
        return _packets;
    }

    public String toLogMessage(boolean detailed) {
        String range = _packets.isEmpty() ? "(Empty batch)" : "(From key " + _packets.get(0).getKey() + " to key " + _packets.get(_packets.size() - 1).getEndKey() + ")";
        return "Reliable async packets completion, packet count [" + _packets.size() + "] " + range + (detailed ? StringUtils.NEW_LINE + ReplicationLogUtils.packetsToLogString(_packets) : "");
    }
}
