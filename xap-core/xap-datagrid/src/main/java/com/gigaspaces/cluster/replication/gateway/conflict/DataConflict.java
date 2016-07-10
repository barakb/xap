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


/**
 * Provides an interface for resolving data conflicts. <p> A data conflict is a conflict caused by
 * write/update/take operations.<br/> The relevant conflicts are: <ul> <li> {@link
 * EntryAlreadyInSpaceConflict} - An attempt to write a new entry which already exists. </li> <li>
 * {@link EntryNotInSpaceConflict} - An attempt to update an existing entry which doesn't exist.
 * </li> <li> {@link EntryLockedUnderTransactionConflict} - An attempt to update an entry which is
 * locked under transaction. </li> <li> {@link EntryVersionConflict} - An attempt to update an entry
 * with a newer/older version than the existing one. </li> </ul> </p>
 *
 * @author eitany
 * @since 8.0.3
 */
public interface DataConflict {
    /**
     * Gets all the conflicted operations, for a non transactional conflict, this will contain a
     * single operation that causes the conflict. If the {@link DataConflict} instance represents a
     * transaction conflict, all of the operation under the transaction are returned, even
     * operations that did not cause any conflict.
     *
     * @return The operations associated with the conflict.
     */
    DataConflictOperation[] getOperations();

    /**
     * Indicates that all of the operations associated with the {@link DataConflict} will be
     * aborted. This is equivalent to iterating over all the operations and calling {@link
     * DataConflictOperation#abort()} on each one.
     */
    void abortAll();

    /**
     * Indicates that all of the operations associated with the {@link DataConflict} will be
     * overridden. This is equivalent to iterating over all the operations and calling {@link
     * DataConflictOperation#override()} on each one. <p> Each operation has a different logic when
     * it gets overridden depends on the conflict that occurred (refer to {@link
     * DataConflictOperation#override()} for more information}. </p>
     */
    void overrideAll();

}
