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

package org.openspaces.test.common.mock;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;
import com.gigaspaces.sync.OperationsBatchData;

import java.util.LinkedList;
import java.util.List;

public class MockOperationsBatchDataBuilder {

    private final List<DataSyncOperation> operations = new LinkedList<DataSyncOperation>();

    public OperationsBatchData build() {
        return new MockOperationsBatchData(operations);
    }

    public MockOperationsBatchDataBuilder clear() {
        operations.clear();
        return this;
    }

    public MockOperationsBatchDataBuilder write(SpaceDocument spaceDoc, String keyName) {
        operations.add(createMockDataSyncOperation(spaceDoc, keyName, DataSyncOperationType.WRITE));
        return this;
    }

    public MockOperationsBatchDataBuilder update(SpaceDocument spaceDoc, String keyName) {
        operations.add(createMockDataSyncOperation(spaceDoc, keyName, DataSyncOperationType.UPDATE));
        return this;
    }

    public MockOperationsBatchDataBuilder remove(SpaceDocument spaceDoc, String keyName) {
        operations.add(createMockDataSyncOperation(spaceDoc, keyName, DataSyncOperationType.REMOVE));
        return this;
    }

    private MockDataSyncOperation createMockDataSyncOperation(SpaceDocument document, String keyName,
                                                              DataSyncOperationType operationType) {
        return new MockDataSyncOperation(
                new SpaceTypeDescriptorBuilder(document.getTypeName()).idProperty(keyName).create(),
                document, operationType);
    }
}
