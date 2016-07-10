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

package com.gigaspaces.persistency.metadata;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.sync.DataSyncOperationType;

/**
 * @author Shadi Massalha
 *
 *         helper class that hold metadata for batch operation
 */
public class BatchUnit {

    private SpaceDocument spaceDocument;
    private String typeName;
    private DataSyncOperationType dataSyncOperationType;

    public SpaceDocument getSpaceDocument() {
        return spaceDocument;
    }

    public String getTypeName() {
        return typeName;
    }

    public DataSyncOperationType getDataSyncOperationType() {
        return dataSyncOperationType;
    }

    public void setSpaceDocument(SpaceDocument spaceDocument) {
        if (spaceDocument == null)
            throw new IllegalArgumentException("spaceDocument can not be null");

        this.spaceDocument = spaceDocument;
        this.typeName = spaceDocument.getTypeName();
    }

    public void setDataSyncOperationType(
            DataSyncOperationType dataSyncOperationType) {
        this.dataSyncOperationType = dataSyncOperationType;
    }

}
