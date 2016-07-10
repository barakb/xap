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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.impl.replica.data.SpaceTypeReplicaData;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * {@link SpaceTypeReplicaDataProducer} creates a replica of the space types.
 *
 * @author anna
 * @version 1.0
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceTypeReplicaDataProducer
        implements ISingleStageReplicaDataProducer<SpaceTypeReplicaData> {
    private final SpaceTypeManager _typeManager;
    private final Iterator<IServerTypeDesc> _typesIterator;
    private boolean _isClosed;
    private List<String> _generatedTypes = new LinkedList<String>();

    public SpaceTypeReplicaDataProducer(SpaceEngine engine) {
        _typeManager = engine.getTypeManager();
        Collection<IServerTypeDesc> types = _typeManager.getSafeTypeTable().values();
        _typesIterator = types.iterator();
    }

    public CloseStatus close(boolean forced) {
        _isClosed = true;
        return CloseStatus.CLOSED;
    }

    public SpaceTypeReplicaData produceNextData(ISynchronizationCallback synchCallback) {
        if (_isClosed)
            return null;

        synchronized (_typeManager.getTypeDescLock()) {
            if (_typesIterator.hasNext()) {
                ITypeDesc typeDesc = _typesIterator.next().getTypeDesc();

                SpaceTypeReplicaData spaceTypeReplicaData = new SpaceTypeReplicaData(typeDesc);

                synchCallback.synchronizationDataGenerated(spaceTypeReplicaData);

                _generatedTypes.add(typeDesc.getTypeName());
                return spaceTypeReplicaData;
            }
        }

        // Any consecutive call should return null, so we close this
        // producer.
        close(false);
        return null;
    }

    public IReplicationFilterEntry toFilterEntry(SpaceTypeReplicaData data) {
        return null;
    }

    public String dumpState() {
        return "Types replica producer: completed [" + _isClosed + "] generated types [" + _generatedTypes + "]";
    }

    @Override
    public String getName() {
        return "SpaceTypeReplicaDataProducer";
    }

}
