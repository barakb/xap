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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter.FilterOperation;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationEntryDataConversionMetadata;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationReadOnlyEntryDataConversionMetadata;

/**
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class ReplicationChannelEntryDataFilterResult {
    public final static ReplicationChannelEntryDataFilterResult PASS = new ReplicationChannelEntryDataFilterResult(FilterOperation.PASS);
    public final static ReplicationChannelEntryDataFilterResult FILTER_DATA = new ReplicationChannelEntryDataFilterResult(FilterOperation.FILTER_DATA);
    public final static ReplicationChannelEntryDataFilterResult FILTER_PACKET = new ReplicationChannelEntryDataFilterResult(FilterOperation.FILTER_PACKET);

    private final FilterOperation _filterOperation;
    private final ReplicationEntryDataConversionMetadata _metadata;

    public final static ReplicationChannelEntryDataFilterResult CONVERT_UPDATE = new ReplicationChannelEntryDataFilterResult(new ReplicationReadOnlyEntryDataConversionMetadata(ReplicationSingleOperationType.UPDATE));
    public final static ReplicationChannelEntryDataFilterResult CONVERT_REMOVE = new ReplicationChannelEntryDataFilterResult(new ReplicationReadOnlyEntryDataConversionMetadata(ReplicationSingleOperationType.REMOVE_ENTRY));
    public final static ReplicationChannelEntryDataFilterResult CONVERT_WRITE = new ReplicationChannelEntryDataFilterResult(new ReplicationReadOnlyEntryDataConversionMetadata(ReplicationSingleOperationType.WRITE));

    public static ReplicationChannelEntryDataFilterResult getConvertToOperationResult(ReplicationEntryDataConversionMetadata metadata) {
        return new ReplicationChannelEntryDataFilterResult(metadata);
    }

    private ReplicationChannelEntryDataFilterResult(ReplicationEntryDataConversionMetadata metadata) {
        if (metadata == null)
            throw new IllegalArgumentException("metadata cannot be null");

        _metadata = metadata;
        _filterOperation = FilterOperation.CONVERT;
    }

    private ReplicationChannelEntryDataFilterResult(FilterOperation filterOperation) {
        this._metadata = null;
        this._filterOperation = filterOperation;
    }


    public FilterOperation getFilterOperation() {
        return _filterOperation;
    }

    public ReplicationSingleOperationType getConvertToOperation() {
        return _metadata.getConvertToOperationType();
    }

    public ReplicationEntryDataConversionMetadata getConversionMetadata() {
        return _metadata;
    }

}
