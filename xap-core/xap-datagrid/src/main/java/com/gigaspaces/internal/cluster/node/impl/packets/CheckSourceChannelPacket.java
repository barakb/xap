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
import com.gigaspaces.internal.cluster.node.impl.groups.NoSuchReplicationGroupExistException;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractGroupNameReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Replication packet that is used by a replication target to check if the source is still
 * considered as the source of that target
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class CheckSourceChannelPacket
        extends AbstractGroupNameReplicationPacket<Boolean> {
    private static final long serialVersionUID = 1L;

    private Object _targetUniqueId;

    private boolean _checkExistanceOnly;

    public CheckSourceChannelPacket() {
    }

    public CheckSourceChannelPacket(String groupName, Object targetUniqueId, boolean checkExistanceOnly) {
        super(groupName);
        _targetUniqueId = targetUniqueId;
        _checkExistanceOnly = checkExistanceOnly;
    }

    public Boolean accept(IIncomingReplicationFacade replicationFacade) {
        try {
            IReplicationSourceGroup sourceGroup = replicationFacade.getReplicationSourceGroup(getGroupName());
            if (sourceGroup != null) {
                String channelName = XapExtensions.getInstance().getReplicationUtils().toChannelName(getSourceLookupName());
                ReplicationEndpointDetails channelEndpointDetails = sourceGroup.getChannelEndpointDetails(channelName);
                final Object existingTargetUniqueId = channelEndpointDetails != null ? channelEndpointDetails.getUniqueId() : null;
                if (!_targetUniqueId.equals(existingTargetUniqueId))
                    return false;
                if (_checkExistanceOnly)
                    return true;
                return sourceGroup.checkChannelConnected(channelName);
            }
            return false;
        } catch (NoSuchReplicationGroupExistException e) {
            return Boolean.FALSE;
        }
    }

    public void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException,
            ClassNotFoundException {
        _targetUniqueId = IOUtils.readObject(in);
        _checkExistanceOnly = in.readBoolean();
    }

    public void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException {
        IOUtils.writeObject(out, _targetUniqueId);
        out.writeBoolean(_checkExistanceOnly);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("targetId", _targetUniqueId);
        textualizer.append("checkExistenceOnly", _checkExistanceOnly);
    }

}
