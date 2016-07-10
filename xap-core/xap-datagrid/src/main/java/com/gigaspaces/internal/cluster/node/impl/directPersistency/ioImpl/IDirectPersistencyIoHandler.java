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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.ioImpl;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyOverflowListSegment;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencyOpInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.admin.DirectPersistencySyncAdminInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.EmbeddedSyncHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin.EmbeddedRelevantGenerationIdsInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin.EmbeddedSyncTransferredInfo;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * The actual i/o of DirectPersistencyOpInfo objects
 *
 * @author yechielf
 * @since 10.2
 */
public interface IDirectPersistencyIoHandler {

    public static final int BULK_SIZE = 1000;

    void insert(IDirectPersistencyOpInfo entry);

    void remove(IDirectPersistencyOpInfo entry);

    void update(IDirectPersistencyOpInfo entry);

    IDirectPersistencyOpInfo get(long generationId, long seq);

    Iterator<IDirectPersistencyOpInfo> iterateOps(boolean currentGeneration);

    //DirectPersistencyOverflowListSegment  methods
    void insert(DirectPersistencyOverflowListSegment segment);

    void remove(DirectPersistencyOverflowListSegment segment);

    void update(DirectPersistencyOverflowListSegment segment);

    DirectPersistencyOverflowListSegment getOverflowSegment(long generationId, long seq);

    Iterator<DirectPersistencyOverflowListSegment> iterateOverflow(boolean currentGeneration);

    void removeOvfBulk(List<DirectPersistencyOverflowListSegment> ovfs);

    void removeOpsBulk(List<IDirectPersistencyOpInfo> ops);

    //admin
    void insert(DirectPersistencySyncAdminInfo ai);

    void update(DirectPersistencySyncAdminInfo ai);

    DirectPersistencySyncAdminInfo getSyncAdminIfExists();

    //++++++++++  embedded list related apis +++++++++++++++++++//

    void removePhantom(String uid, boolean checkExistance, long generationId, long seq);

    void insert(EmbeddedSyncTransferredInfo ai);

    void update(EmbeddedSyncTransferredInfo ai);

    void remove(EmbeddedSyncTransferredInfo ai);

    List<EmbeddedSyncTransferredInfo> getAllTransferredInfo(EmbeddedSyncHandler h, Collection<Long> generations);

    EmbeddedRelevantGenerationIdsInfo getEmbeddedRelevantGenerationIdsInfo();

    void update(EmbeddedRelevantGenerationIdsInfo ai);

    void insert(EmbeddedRelevantGenerationIdsInfo ai);


}
