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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.EmbeddedSyncHandler;

import java.util.Collection;

/**
 * Embedded Sync- info regarding generation ids from which not all embedded info used surly
 * transferred to the sync list
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedRelevantGenerationIdsHandler {

    private final EmbeddedSyncHandler _embeddedHandler;
    private final long _currentGenerationId;
    private EmbeddedRelevantGenerationIdsInfo _info;

    private Collection<EmbeddedSyncTransferredInfo> _allTransferredInfo;

    public EmbeddedRelevantGenerationIdsHandler(EmbeddedSyncHandler embeddedHandler) {
        _embeddedHandler = embeddedHandler;
        _currentGenerationId = embeddedHandler.getMainSyncHandler().getCurrentGenerationId();
    }

    public void afterInitializedBlobStoreIO() {
        EmbeddedRelevantGenerationIdsInfo info = _embeddedHandler.getMainSyncHandler().getListHandler().getIoHandler().getEmbeddedRelevantGenerationIdsInfo();
        if (info == null) {
            info = new EmbeddedRelevantGenerationIdsInfo(_currentGenerationId);
            _embeddedHandler.getMainSyncHandler().getListHandler().getIoHandler().insert(info);
        } else {
            info.addCurrentGeneration(_currentGenerationId);
            _embeddedHandler.getMainSyncHandler().getListHandler().getIoHandler().update(info);
        }
        _info = info;
        _allTransferredInfo = _embeddedHandler.getMainSyncHandler().getListHandler().getIoHandler().getAllTransferredInfo(_embeddedHandler, getRelevantGenerations());
    }

    public void initialize() {
    }

    public Collection<EmbeddedSyncTransferredInfo> getAllTransferredInfo() {
        return _allTransferredInfo;
    }


    public boolean isRelevant(long gen) {
        return gen == _currentGenerationId || _info.isRelevant(gen);
    }

    //called after recovery or when all records transferred to main list
    public void removeOldGens() {
        if (_info.removeOldGens(_currentGenerationId)) {
            _embeddedHandler.getMainSyncHandler().getListHandler().getIoHandler().update(_info);
        }
    }

    public Collection<Long> getRelevantGenerations() {
        return _info.getRelevantGenerations();
    }

}
