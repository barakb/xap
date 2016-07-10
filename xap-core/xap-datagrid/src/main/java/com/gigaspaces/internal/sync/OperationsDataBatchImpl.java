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

package com.gigaspaces.internal.sync;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SynchronizationSourceDetails;

/**
 * @author idan
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class OperationsDataBatchImpl
        implements OperationsBatchData {
    private final DataSyncOperation[] _operations;
    private final String _sourceName;

    public OperationsDataBatchImpl(DataSyncOperation operation, String sourceName) {
        this(new DataSyncOperation[]{operation}, sourceName);
    }

    public OperationsDataBatchImpl(DataSyncOperation[] operation, String spaceName) {
        _operations = operation;
        _sourceName = spaceName;
    }

    @Override
    public DataSyncOperation[] getBatchDataItems() {
        return _operations;
    }

    @Override
    public SynchronizationSourceDetails getSourceDetails() {
        return new SynchronizationSourceDetails() {
            @Override
            public String getName() {
                return _sourceName;
            }
        };
    }

}
