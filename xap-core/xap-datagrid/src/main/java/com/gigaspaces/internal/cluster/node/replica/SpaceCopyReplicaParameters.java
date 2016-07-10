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

package com.gigaspaces.internal.cluster.node.replica;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncListFetcher;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class SpaceCopyReplicaParameters implements ISpaceCopyReplicaParameters {
    private static final long serialVersionUID = 1L;

    public static enum ReplicaType {SYNCRONIZE, COPY, BROADCAST_NOTIFY_TEMPLATES_COPY}

    private boolean _copyNotifyTemplates;
    private boolean _isTransient;
    private boolean _memoryOnly;
    private boolean _includeEvictionReplicationMarkers;
    private LinkedList<ITemplatePacket> _templatePackets = new LinkedList<ITemplatePacket>(); //used to copy only the data that matches the template condition

    private ReplicaType _replicaType;
    private SpaceContext _spaceContext;
    private DirectPersistencySyncListFetcher _fetcher;
    private transient List<String> _syncList;

    public SpaceCopyReplicaParameters() {
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(_copyNotifyTemplates);
        //Handle case before 9.1, we had no memory only, however, if we turn on memory only it comes from code path the logically previously
        //was when isTransient was true
        if (_memoryOnly && LRMIInvocationContext.getEndpointLogicalVersion().lessThan(PlatformLogicalVersion.v9_1_0))
            _isTransient = true;

        out.writeBoolean(_isTransient);
        IOUtils.writeObject(out, _templatePackets);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0)) {
            out.writeBoolean(_memoryOnly);
            out.writeBoolean(_includeEvictionReplicationMarkers);
        }
        IOUtils.writeObject(out, _replicaType);
        IOUtils.writeObject(out, _spaceContext);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v10_2_0)) {
            IOUtils.writeObject(out, _fetcher);
        }

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _copyNotifyTemplates = in.readBoolean();
        _isTransient = in.readBoolean();
        _templatePackets = IOUtils.readObject(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_1_0)) {
            _memoryOnly = in.readBoolean();
            _includeEvictionReplicationMarkers = in.readBoolean();
        } else {
            _memoryOnly = _isTransient;
            _includeEvictionReplicationMarkers = false;
        }
        _replicaType = IOUtils.readObject(in);
        _spaceContext = IOUtils.readObject(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v10_2_0)) {
            _fetcher = IOUtils.readObject(in);
        }
    }

    public boolean isCopyNotifyTemplates() {
        return _copyNotifyTemplates;
    }

    public boolean isTransient() {
        return _isTransient;
    }

    public void setCopyNotifyTemplates(boolean copyNotifyTemplates) {
        _copyNotifyTemplates = copyNotifyTemplates;
    }

    public void setTransient(boolean isTransient) {
        _isTransient = isTransient;
    }

    public void addTemplatePacket(ITemplatePacket templatePacket) {
        _templatePackets.add(templatePacket);
    }

    public List<ITemplatePacket> getTemplatePackets() {
        return _templatePackets;
    }

    public ReplicaType getReplicaType() {
        return _replicaType;
    }

    public void setReplicaType(ReplicaType replicaType) {
        _replicaType = replicaType;
    }

    public SpaceContext getSpaceContext() {
        return _spaceContext;
    }

    public void setSpaceContext(SpaceContext spaceContext) {
        _spaceContext = spaceContext;
    }

    public boolean isMemoryOnly() {
        return _memoryOnly;
    }

    public void setMemoryOnly(boolean memoryOnly) {
        _memoryOnly = memoryOnly;
    }

    public void setIncludeEvictionReplicationMarkers(boolean includeReplicationMarkers) {
        _includeEvictionReplicationMarkers = includeReplicationMarkers;
    }

    public void setSynchronizationListFetcher(DirectPersistencySyncListFetcher fetcher) {
        this._fetcher = fetcher;
    }

    public DirectPersistencySyncListFetcher getSynchronizationListFetcher() {
        return _fetcher;
    }

    public List<String> getSyncList() {
        return _syncList;
    }

    public void setSyncList(List<String> _syncList) {
        this._syncList = _syncList;
    }

    public boolean isIncludeEvictionReplicationMarkers() {
        return _includeEvictionReplicationMarkers;
    }

    @Override
    public String toString() {
        return "ReplicaType=" + _replicaType + StringUtils.NEW_LINE +
                "CopyNotifyTemplate=" + _copyNotifyTemplates + StringUtils.NEW_LINE +
                "IsTransient=" + _isTransient + StringUtils.NEW_LINE +
                "MemoryOnly=" + _memoryOnly + StringUtils.NEW_LINE +
                "IncludeEvictionReplicationMarkers=" + _includeEvictionReplicationMarkers + StringUtils.NEW_LINE +
                "TemplatePackets=" + _templatePackets + StringUtils.NEW_LINE +
                "SpaceContext=" + String.valueOf(_spaceContext != null);
    }


}
