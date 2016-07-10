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

//
package com.gigaspaces.server.blobstore;


@com.gigaspaces.api.InternalApi
public class BlobStoreAddBulkOperationResult extends BlobStoreBulkOperationResult {

    public BlobStoreAddBulkOperationResult(java.io.Serializable id, Object position) {
        super(BlobStoreBulkOperationType.ADD, id, null, position);
    }

    public BlobStoreAddBulkOperationResult(java.io.Serializable id, Throwable exception) {
        super(BlobStoreBulkOperationType.ADD, id, exception);
    }

}
