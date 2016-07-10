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

package com.gigaspaces.sync.change;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;
import com.j_spaces.core.filters.entry.IFilterEntry;

/**
 * Used to extract the {@link DataSyncChangeSet} from operations that represent a change operation.
 * See {@link DataSyncOperation}, {@link DataSyncOperationType#CHANGE}, {@link
 * IReplicationFilterEntry} and {@link ReplicationOperationType#CHANGE}.
 *
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class ChangeDataSyncOperation {
    /**
     * @return the {@link DataSyncChangeSet} from a {@link DataSyncOperation} which represents a
     * {@link DataSyncOperationType#CHANGE} operation. If called on a {@link DataSyncOperation}
     * which isn't a change operation, an exception will be thrown
     */
    public static DataSyncChangeSet getChangeSet(DataSyncOperation dataSyncOperation) {
        if (!(dataSyncOperation instanceof DataSyncChangeSet))
            throw new IllegalArgumentException("The provided DataSyncOperation [" + dataSyncOperation + "] is not of change operation type");

        return (DataSyncChangeSet) dataSyncOperation;
    }

    /**
     * @return the {@link DataSyncChangeSet} from a {@link IFilterEntry} which represents a {@link
     * ReplicationOperationType#CHANGE} operation. If called on a {@link IFilterEntry} which isn't a
     * change operation, an exception will be thrown. This method is used within a space replication
     * filter or a space filter.
     */
    public static DataSyncChangeSet getChangeSet(IFilterEntry filterEntry) {
        if (!(filterEntry instanceof DataSyncChangeSet))
            throw new IllegalArgumentException("The provided IReplicationFilterEntry [" + filterEntry + "] is not of change operation type");

        return (DataSyncChangeSet) filterEntry;
    }

}
