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

package com.gigaspaces.cluster.loadbalance;

import com.gigaspaces.internal.query.IPartitionResultMetadata;
import com.j_spaces.core.UidQueryPacket;
import com.j_spaces.core.client.GSIterator;

/**
 * PartitionResultsMetadata is meta data attached to the multiple uids entry/template.<br> Since
 * uids are just strings, they lose all the information about the space they came for.<br>
 * PartitionResultsMetadata keeps this data so it can be used to route the uids to specific
 * partition,<br> instead doing broadcast.<br> Used by {@link GSIterator}.<br> See {@link
 * UidQueryPacket}
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class PartitionResultsMetadata implements IPartitionResultMetadata {
    private final Integer _partitionId;
    private final int _numOfResults;

    public PartitionResultsMetadata(Integer partitionId, int numOfResults) {
        _partitionId = partitionId;
        _numOfResults = numOfResults;
    }

    @Override
    public Integer getPartitionId() {
        return _partitionId;
    }

    @Override
    public int getNumOfResults() {
        return _numOfResults;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("  MultipleUidsMetaData\n    [\n        getPartitionId()=");
        builder.append(getPartitionId());
        builder.append(", \n        getDataSize()=");
        builder.append(getNumOfResults());
        builder.append("\n    ]");
        return builder.toString();
    }

}
