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

package com.gigaspaces.internal.cluster.node.impl.packets.data.filters;

import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.sync.change.ChangeOperation;
import com.gigaspaces.sync.change.DataSyncChangeSet;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.util.Collection;

/**
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class ChangeReplicationFilterUidDataWrapper
        extends ReplicationFilterUidDataWrapper
        implements IReplicationFilterEntry, DataSyncChangeSet {

    private static final long serialVersionUID = 1L;

    private final int _version;
    private final Collection _spaceEntryMutators;
    private final Object _id;

    public ChangeReplicationFilterUidDataWrapper(
            AbstractReplicationPacketSingleEntryData data, String uid,
            ITypeDesc typeDesc, ReplicationOperationType operationType,
            int objectType, ITimeToLiveUpdateCallback timeToLiveCallback, int version, Collection spaceEntryMutators, Object id) {
        super(data, uid, typeDesc, operationType, objectType, timeToLiveCallback);
        _version = version;
        _spaceEntryMutators = spaceEntryMutators;
        _id = id;
    }

    @Override
    public int getVersion() {
        return _version;
    }

    @Override
    public Collection<ChangeOperation> getOperations() {
        return _spaceEntryMutators;
    }

    @Override
    public Object getId() {
        return _id;
    }


}
