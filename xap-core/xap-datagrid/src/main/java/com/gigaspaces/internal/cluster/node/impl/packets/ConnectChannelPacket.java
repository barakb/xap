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
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractGroupNameReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Replication packet that is used to establish replication channels between a {@link
 * IReplicationSourceGroup} and a {@link IReplicationTargetGroup}
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ConnectChannelPacket extends AbstractGroupNameReplicationPacket<Object> {
    private static final long serialVersionUID = 1L;

    private RouterStubHolder _routerStubHolder;
    private ConnectChannelHandshakeRequest _channelHandshakeRequest;


    //For externalizable
    public ConnectChannelPacket() {
    }


    public ConnectChannelPacket(String groupName, RouterStubHolder routerStubHolder, ConnectChannelHandshakeRequest channelHandshakeRequest) {
        super(groupName);
        _routerStubHolder = routerStubHolder;
        _channelHandshakeRequest = channelHandshakeRequest;
    }


    public Object accept(IIncomingReplicationFacade replicationFacade) {
        IReplicationTargetGroup target = replicationFacade.getReplicationTargetGroup(getGroupName());
        ConnectChannelHandshakeResponse connectChannelHandshakeResponse = target.connectChannel(_routerStubHolder, _channelHandshakeRequest);
        PlatformLogicalVersion platformLogicalVersion = _routerStubHolder.getMyEndpointDetails().getPlatformLogicalVersion();
        if (platformLogicalVersion != null)
            return connectChannelHandshakeResponse;

        return connectChannelHandshakeResponse.getProcessLogHandshakeResponse();
    }

    public ConnectChannelHandshakeRequest getChannelHandshakeRequest() {
        return _channelHandshakeRequest;
    }

    public RouterStubHolder getRouterStubHolder() {
        return _routerStubHolder;
    }

    public void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException,
            ClassNotFoundException {
        _routerStubHolder = IOUtils.readObject(in);
        _channelHandshakeRequest = new ConnectChannelHandshakeRequest();
        _channelHandshakeRequest.readExternal(in);
    }

    public void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException {
        IOUtils.writeObject(out, _routerStubHolder);
        _channelHandshakeRequest.writeExternal(out);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("handshakeRequest", _channelHandshakeRequest);
    }

}
