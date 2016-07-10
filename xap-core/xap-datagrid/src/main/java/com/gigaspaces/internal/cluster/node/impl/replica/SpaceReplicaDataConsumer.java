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

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

@com.gigaspaces.api.InternalApi
public class SpaceReplicaDataConsumer
        implements ISpaceReplicaDataConsumer<IExecutableSpaceReplicaData, SpaceCopyIntermediateResult> {

    private final ISpaceReplicaConsumeFacade _consumeFacade;
    private final SpaceTypeManager _typeManager;

    public SpaceReplicaDataConsumer(SpaceTypeManager typeManager, ISpaceReplicaConsumeFacade consumeFacade) {
        _typeManager = typeManager;
        _consumeFacade = consumeFacade;
    }

    public void consumeData(IExecutableSpaceReplicaData data,
                            SpaceCopyIntermediateResult intermediateResult, IIncomingReplicationFacade facade) throws Exception {
        data.execute(_consumeFacade, intermediateResult, facade);
    }

    public SpaceCopyIntermediateResult createEmptyResult() {
        return new SpaceCopyIntermediateResult();
    }

    public IReplicationFilterEntry toFilterEntry(
            IExecutableSpaceReplicaData data) {
        return data.toFilterEntry(_typeManager);
    }

}
