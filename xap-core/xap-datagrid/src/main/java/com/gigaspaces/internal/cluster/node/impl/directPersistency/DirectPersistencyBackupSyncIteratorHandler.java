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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import com.j_spaces.kernel.SystemProperties;

import java.util.Iterator;

/**
 * @author Boris
 * @since 10.2.0 Encapsulates the sync list iterator for the backup, used for fetching the sync list
 * (primary asks backup for it's sync list)
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencyBackupSyncIteratorHandler {
    private Iterator<String> entriesForRecover;
    private final int batchSize = Integer.getInteger(SystemProperties.REPLICATION_BLOBSTORE_SYNC_LIST_BATCH_SIZE,
            SystemProperties.REPLICATION_BLOBSTORE_SYNC_LIST_BATCH_SIZE_DEFAULT);

    public DirectPersistencyBackupSyncIteratorHandler(Iterator<String> entriesForRecover) {
        this.entriesForRecover = entriesForRecover;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public DirectPersistencySyncListBatch getNextBatch() {
        int currentBatchSize = getBatchSize();
        DirectPersistencySyncListBatch batch = new DirectPersistencySyncListBatch();
        while (currentBatchSize > 0 && entriesForRecover.hasNext()) {
            batch.addEntryToBatch(entriesForRecover.next());
            currentBatchSize--;
        }
        return batch;
    }
}
