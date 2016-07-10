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

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;

import java.util.Collection;

/**
 * Contains details for a {@link DataSyncOperation} of type {@link DataSyncOperationType#CHANGE}.
 *
 * @author eitany
 * @since 9.5
 */
public interface DataSyncChangeSet {
    /**
     * @return the change operations.
     */
    Collection<ChangeOperation> getOperations();

    /**
     * @return the id of the object which was changed.
     */
    Object getId();

    /**
     * @return the version of the object after the change was applied.
     */
    int getVersion();

    /**
     * @return the remaining time to live of the object after the change was applied.
     */
    long getTimeToLive();
}
