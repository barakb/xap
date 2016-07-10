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

import com.gigaspaces.client.ChangeSet;

/**
 * A single change applied on a {@link ChangeSet} when using a change operation on an object. See
 * the following operations to obtain the relevant constant values in order to extract the data from
 * the change operation
 * <pre>
 * {@link IncrementOperation}
 * {@link AddToCollectionOperation}
 * {@link AddAllToCollectionOperation}
 * {@link RemoveFromCollectionOperation}
 * {@link PutInMapOperation}
 * {@link RemoveFromMapOperation}
 * {@link SetOperation}
 * {@link UnsetOperation}
 * </pre>
 * <p>
 * <pre>
 * <code>
 * DataSyncChangeSet dataSyncChangeSet = ChangeDataSyncOperation.getChangeSet(dataSyncOperation);
 * Collection<ChangeOperation> operations = dataSyncChangeSet.getOperations();
 * for(ChangeOperation operation : operations){
 *   if (SetOperation.NAME.equals(operation.getName()){
 *     String path = SetOperation.getPath(operation);
 *     Object value = SetOperation.getValue(operation);
 *     // ... do something with the path and value
 *   }
 *   //...
 * }
 * </code>
 * </pre>
 * </p>
 *
 * @author eitany
 * @since 9.5
 */
public interface ChangeOperation {
    /**
     * @return the name of the operation.
     */
    String getName();

}
