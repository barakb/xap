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

package com.gigaspaces.internal.client;

import com.gigaspaces.internal.query.IPartitionResultMetadata;
import com.gigaspaces.internal.utils.CollectionUtils;

import java.util.List;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntriesUidsResult {

    private final String[] _uids;
    private final List<IPartitionResultMetadata> _partitionResultMetadata;

    public ReadTakeEntriesUidsResult(String[] uids, IPartitionResultMetadata metadata) {
        this(uids, CollectionUtils.toList(metadata));
    }

    public ReadTakeEntriesUidsResult(String[] uids, List<IPartitionResultMetadata> metadata) {
        this._uids = uids;
        this._partitionResultMetadata = metadata;
    }

    public String[] getUids() {
        return _uids;
    }

    public List<IPartitionResultMetadata> getPartitionsMetadata() {
        return _partitionResultMetadata;
    }

}
