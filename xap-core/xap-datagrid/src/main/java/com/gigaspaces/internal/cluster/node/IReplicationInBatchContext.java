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

package com.gigaspaces.internal.cluster.node;

import java.util.List;

public interface IReplicationInBatchContext
        extends IReplicationInContext {

    /**
     * Should be called once all the pending operation that are kept in the context are consumed and
     * considered successfully replicated. once called all the pending operations that are kept in
     * the context are cleared
     */
    void pendingConsumed();

    <T> void addPendingContext(T operationContext);

    <T> List<T> getPendingContext();

    void currentConsumed();

    <T> void setTagObject(T tagObject);

    <T> T getTagObject();

    int getEntireBatchSize();
}
