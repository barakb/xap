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

package com.gigaspaces.client;

import com.gigaspaces.sync.change.AddAllToCollectionOperation;
import com.gigaspaces.sync.change.AddToCollectionOperation;
import com.gigaspaces.sync.change.ChangeOperation;
import com.gigaspaces.sync.change.IncrementOperation;
import com.gigaspaces.sync.change.PutInMapOperation;
import com.gigaspaces.sync.change.RemoveFromCollectionOperation;
import com.gigaspaces.sync.change.RemoveFromMapOperation;
import com.gigaspaces.sync.change.SetOperation;
import com.gigaspaces.sync.change.UnsetOperation;

import java.io.Serializable;

/**
 * A single change result which was applied on an object. See the following operations to obtain the
 * relevant constant values in order to extract the data from the change operation
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
 * ChangeResult<MyPojo> changeResult = gigaSpace.change(new SQLQuery<MyPojo>(MyPojo.class,
 * "someField = 'someValue'),
 *                                                      new ChangeSet().increment("counterField",
 * 1), ChangeModifiers.RETURN_DETAILED_RESULTS)
 * for (ChangedEntryDetails<MyPojo> changeEntryDetails : changeResult.getResults()) {
 *   List<ChangeOperationResult> operations = changeEntryDetails.getChangeOperationsResults();
 *   for(ChangeOperationResult operationResult : operations) {
 *     if (IncrementOperation.represents(operationResult.getOperation()) {
 *       String path = IncrementOperation.getPath(operationResult.getOperation());
 *       Number delta = IncrementOperation.getDelta(operationResult.getOperation());
 *       Number result = IncrementOperation.getNewValue(operationResult);
 *       // ... do something with the result, path and delta
 *     }
 *   }
 *   //...
 * }
 * </code>
 * </pre>
 * </p>
 *
 * @author eitany
 * @since 9.7
 */
public interface ChangeOperationResult {
    /**
     * Returns this change operation
     */
    ChangeOperation getOperation();

    /**
     * Returns the result of this change operation an a specific entry
     */
    Serializable getResult();
}
