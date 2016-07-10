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

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;


/**
 * Provides an interface for handling a conflicting data operation. <p> Conflicting operations can
 * be resolved using the {@link DataConflictOperation#abort()} and {@link
 * DataConflictOperation#override()} methods. </p> <p> It is important to understand that {@link
 * DataConflictOperation#override()} acts differently for each {@link OperationType} (refer to
 * {@link ConflictCause} implementations for more information). </p>
 *
 * @author eitany
 * @author idan
 * @since 8.0.3
 */
public interface DataConflictOperation extends DataSyncOperation {
    /**
     * Operation type enum.
     *
     * @author eitany
     * @since 8.0.3
     * @deprecated since 9.0.1 - {@link DataSyncOperationType}.
     */
    @Deprecated
    public enum OperationType {
        /**
         * Write operation.
         */
        WRITE,
        /**
         * Update operation.
         */
        UPDATE,
        /**
         * Partial update operation.
         */
        PARTIAL_UPDATE,
        /**
         * Remove operation.
         */
        REMOVE,
        /**
         * Remove by UID operation.
         */
        REMOVE_BY_UID,
        /**
         * Change operation.
         */
        CHANGE
    }

    /**
     * @return The operation type.
     * @deprecated since 9.0.1 - use {@link #getDataSyncOperationType()} instead.
     */
    @Deprecated
    OperationType getOperationType();

    /**
     * @return The {@link ConflictCause} instance representing the conflict.
     */
    ConflictCause getConflictCause();

    /**
     * @return true if that operation had a conflict, otherwise false.
     */
    boolean hasConflict();

    /**
     * @return The user entry the operation had a conflict for.
     * @deprecated since 9.0.1 - use {@link #getDataAsDocument()} or {@link #getDataAsObject()}
     * instead.
     */
    @Deprecated
    Object getOperationEntry();

    /**
     * Determines that the operation will be aborted. This can only be used if the {@link
     * #supportsAbort()} returns true.
     */
    void abort();

    /**
     * Determines that the operation will be overridden. This can only be used if the {@link
     * #supportsOverride()} returns true. <p> It is possible to change the operation's entry data
     * using the {@link #getDataAsObject()} or {@link #getDataAsDocument()} methods. <p> <ul>
     * <li>Write operation with an {@link EntryAlreadyInSpaceConflict} will turn to an update
     * operation.</li> <li>Update operation with an {@link EntryNotInSpaceConflict} will turn to a
     * write operation.</li> <li>Update operation with an {@link EntryVersionConflict} will
     * overwrite the entry in the target space.</li> <li>Update operation with an {@link
     * EntryLockedUnderTransactionConflict} will be retried.</li> </ul> </p>
     */
    void override();

    /**
     * @return true if overriding the operation is supported, otherwise false.
     */
    boolean supportsOverride();

    /**
     * @return true of aborting the operation is supported, otherwise false.
     */
    boolean supportsAbort();

    /**
     * @return The current resolve attempt number for the operation.
     */
    int getResolveAttempt();

}
