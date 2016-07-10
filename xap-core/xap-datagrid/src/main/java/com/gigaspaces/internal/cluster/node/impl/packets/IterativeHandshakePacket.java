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
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractGroupNameReplicationPacket;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class IterativeHandshakePacket
        extends AbstractGroupNameReplicationPacket<Object> {
    private static final long serialVersionUID = 1L;
    private IHandshakeIteration _handshakeIteration;

    public IterativeHandshakePacket() {
    }

    public IterativeHandshakePacket(String groupName, IHandshakeIteration handshakeIteration) {
        super(groupName);
        _handshakeIteration = handshakeIteration;
    }

    public Object accept(IIncomingReplicationFacade incomingReplicationFacade) {
        IReplicationTargetGroup targetGroup = incomingReplicationFacade.getReplicationTargetGroup(getGroupName());
        targetGroup.processHandshakeIteration(getSourceLookupName(), getSourceUniqueId(), _handshakeIteration);
        return null;
    }

    public void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException,
            ClassNotFoundException {
        _handshakeIteration = IOUtils.readObject(in);
    }

    public void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException {
        IOUtils.writeObject(out, _handshakeIteration);
    }

    public IHandshakeIteration getHandshakeIteration() {
        return _handshakeIteration;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("handshakeIteration", _handshakeIteration);
    }

}
