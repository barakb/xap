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

import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.EmbeddedSyncSegment;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.IEmbeddedSyncOpInfo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Embedded Sync- info regarding transferred records per segment
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedSyncListTransferredInfoHandler {
    private final int _segmentNumber;
    private final EmbeddedSyncSegment _segment;
    private final EmbeddedSyncTransferredInfo _currentGenInfo;
    private volatile Map<Long, EmbeddedSyncTransferredInfo> _oldGenerationsInfo;

    public EmbeddedSyncListTransferredInfoHandler(EmbeddedSyncSegment segment) {
        _segment = segment;
        _segmentNumber = segment.getSegmentNumber();
        _currentGenInfo = new EmbeddedSyncTransferredInfo(_segment.getEmbeddedHandler().getMainSyncHandler().getCurrentGenerationId(), _segment.getSegmentNumber());
        _oldGenerationsInfo = new HashMap<Long, EmbeddedSyncTransferredInfo>();

    }

    public void afterInitializedBlobStoreIO(Collection<Long> oldGens) {
        Collection<EmbeddedSyncTransferredInfo> oldGensInfo = _segment.getEmbeddedHandler().getGensHandler().getAllTransferredInfo();
        if (oldGensInfo != null) {
            for (EmbeddedSyncTransferredInfo oi : oldGensInfo)
                if (_segmentNumber == ((oi.getSegmentNum()) % _segment.getEmbeddedHandler().getNumSegments()))
                    _oldGenerationsInfo.put(oi.getGenerationId(), oi);
        }
        //write the current gen info record
        _segment.getEmbeddedHandler().getMainSyncHandler().getListHandler().getIoHandler().insert(_currentGenInfo);
    }

    public void initialize() {

    }

    //record was transferred from embedded to the regular list- update the info for its segment
    public void updateTranferredInfo(IEmbeddedSyncOpInfo oi) {
        if (_currentGenInfo.updateTranferredInfo(oi.getSeqInEmbeddedSegment())) {//need to update the info record
            _segment.getEmbeddedHandler().getMainSyncHandler().getListHandler().getIoHandler().update(_currentGenInfo);
        }

    }


    public boolean isSeqNumberTransferredForSure(long genId, int seqNumWithinSegment) {
        if (genId == _segment.getEmbeddedHandler().getMainSyncHandler().getCurrentGenerationId())
            return _currentGenInfo.isSeqNumberCellTransferred(seqNumWithinSegment);
        EmbeddedSyncTransferredInfo o = _oldGenerationsInfo.get(genId);
        if (o == null)
            return true;
        return
                o.isSeqNumberCellTransferred(seqNumWithinSegment);
    }

    public void removeOldGenerationsTransferredInfo() {
        Map<Long, EmbeddedSyncTransferredInfo> oldGenerationsInfo = _oldGenerationsInfo;
        _oldGenerationsInfo = new HashMap<Long, EmbeddedSyncTransferredInfo>();
        Iterator<EmbeddedSyncTransferredInfo> iter = oldGenerationsInfo.values().iterator();
        while (iter.hasNext()) {
            EmbeddedSyncTransferredInfo inf = iter.next();
            _segment.getEmbeddedHandler().getMainSyncHandler().getListHandler().getIoHandler().remove(inf);
        }
    }

}
