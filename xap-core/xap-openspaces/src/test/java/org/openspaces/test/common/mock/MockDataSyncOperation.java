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
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;

public class MockDataSyncOperation implements DataSyncOperation {
    private final SpaceDocument document;
    private final DataSyncOperationType operationType;
    private final SpaceTypeDescriptor typeDescriptor;

    public MockDataSyncOperation(SpaceTypeDescriptor typeDescriptor, SpaceDocument document, DataSyncOperationType operationType) {
        this.typeDescriptor = typeDescriptor;
        this.document = document;
        this.operationType = operationType;
    }

    public String getUid() {
        return null;
    }

    public DataSyncOperationType getDataSyncOperationType() {
        return operationType;
    }

    public Object getDataAsObject() {
        return null;
    }

    public SpaceDocument getDataAsDocument() {
        return document;
    }

    public SpaceTypeDescriptor getTypeDescriptor() {
        return typeDescriptor;
    }

    public boolean supportsGetTypeDescriptor() {
        return true;
    }

    public boolean supportsDataAsObject() {
        return false;
    }

    public boolean supportsDataAsDocument() {
        return true;
    }

    @Override
    public Object getSpaceId() {
        return null;
    }

    @Override
    public boolean supportsGetSpaceId() {
        return false;
    }
}
