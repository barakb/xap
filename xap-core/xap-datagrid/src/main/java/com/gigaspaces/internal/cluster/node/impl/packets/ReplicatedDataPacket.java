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
import com.gigaspaces.internal.cluster.node.impl.router.AbstractGroupNameReplicationPacket;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class ReplicatedDataPacket
        extends AbstractGroupNameReplicationPacket<Object> {
    private static final long serialVersionUID = 1L;

    private IReplicationOrderedPacket _packet;

    private transient boolean _clean = true;

    public ReplicatedDataPacket() {
    }

    public ReplicatedDataPacket(String groupName) {
        super(groupName);
    }

    public void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException {
        IOUtils.writeObject(out, _packet);
    }

    public void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException,
            ClassNotFoundException {
        _packet = IOUtils.readObject(in);
    }

    public Object accept(IIncomingReplicationFacade incomingReplicationFacade) {
        IReplicationTargetGroup targetGroup = incomingReplicationFacade.getReplicationTargetGroup(getGroupName());
        return targetGroup.process(getSourceLookupName(), getSourceUniqueId(), _packet);
    }

    public IReplicationOrderedPacket getPacket() {
        return _packet;
    }

    public void setPacket(IReplicationOrderedPacket packet) {
        if (!_clean)
            throw new IllegalStateException("Attempt to override packet when it was not released");
        _packet = packet;
        _clean = false;
    }

    public void clean() {
        _clean = true;
        _packet = null;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("packet", _packet);
    }

}
