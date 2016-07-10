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

package com.gigaspaces.internal.cluster.node.impl.replica.data;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarkerWireForm;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters.ReplicaType;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class EntrySynchronizeReplicaData
        extends AbstractEntryReplicaData

{
    private static final long serialVersionUID = 1L;
    private transient IMarker _marker;
    private IMarkerWireForm _markerWireForm;

    public EntrySynchronizeReplicaData() {
    }

    public EntrySynchronizeReplicaData(IEntryPacket entryPacket, IMarker marker) {
        super(entryPacket);
        _marker = marker;
    }

    @Override
    public ReplicaType getReplicaType() {
        return ReplicaType.SYNCRONIZE;
    }

    @Override
    protected IMarker getMarker(IIncomingReplicationFacade incomingReplicationFacade) {
        if (_markerWireForm == null)
            return null;

        String groupName = _markerWireForm.getGroupName();
        IReplicationSourceGroup sourceGroup = incomingReplicationFacade.getReplicationSourceGroup(groupName);
        return _markerWireForm.toFinalizedForm(sourceGroup.getGroupBacklog());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0)) {
            IMarkerWireForm markerWireForm = _marker != null ? _marker.toWireForm() : null;
            IOUtils.writeObject(out, markerWireForm);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            _markerWireForm = IOUtils.readObject(in);
    }

    @Override
    public String toString() {
        String markerStr = null;
        if (_marker != null)
            markerStr = _marker.toString();
        else if (_markerWireForm != null)
            markerStr = _markerWireForm.toString();
        return super.toString() + (markerStr != null ? " " + markerStr : "");
    }

}
