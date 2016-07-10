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

package com.gigaspaces.internal.cluster.node.impl.packets;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractGroupNameReplicationPacket;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class HandleDataConsumeErrorPacket
        extends AbstractGroupNameReplicationPacket<IDataConsumeFix> {
    private static final long serialVersionUID = 1L;
    private IDataConsumeResult _errorResult;
    private long _errorDataPacketKey;

    public HandleDataConsumeErrorPacket() {
    }

    public HandleDataConsumeErrorPacket(IDataConsumeResult errorResult, String groupName, long errorDataPacketKey) {
        super(groupName);
        _errorResult = errorResult;
        _errorDataPacketKey = errorDataPacketKey;
    }

    public IDataConsumeFix accept(IIncomingReplicationFacade incomingReplicationFacade) {
        IReplicationSourceGroup sourceGroup = incomingReplicationFacade.getReplicationSourceGroup(getGroupName());
        IReplicationGroupBacklog backlog = sourceGroup.getGroupBacklog();
        IReplicationPacketData<?> errorData = null;
        if (_errorDataPacketKey != -1) {
            IReplicationOrderedPacket packet = backlog.getSpecificPacket(_errorDataPacketKey);
            if (packet != null)
                errorData = packet.getData();
        }
        //Create a fix for the error
        return backlog.getDataProducer().createFix(_errorResult, errorData);
    }

    public void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException,
            ClassNotFoundException {
        _errorResult = IOUtils.readObject(in);
        _errorDataPacketKey = in.readLong();
    }

    public void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException {
        IOUtils.writeObject(out, _errorResult);
        out.writeLong(_errorDataPacketKey);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("errorResult", _errorResult);
        textualizer.append("errorPacketKey", _errorDataPacketKey);
    }

}
