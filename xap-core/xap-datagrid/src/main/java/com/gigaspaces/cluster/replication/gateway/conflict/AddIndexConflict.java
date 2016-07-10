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


package com.gigaspaces.cluster.replication.gateway.conflict;

import com.gigaspaces.metadata.index.SpaceIndex;

/**
 * Provides an interface for handling an {@link AddIndexesConflict}.
 *
 * @author eitany
 * @since 8.0.3
 */
public interface AddIndexConflict {
    /**
     * @return {@link ConflictCause} instance representing the conflict that occurred.
     */
    ConflictCause getConflictCause();

    /**
     * @return The type name the add index operation was executed for.
     */
    String getTypeName();

    /**
     * @return The new indexes the add index operation failed to add.
     */
    SpaceIndex[] getIndexes();
}
