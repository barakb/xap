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


package org.openspaces.core;

import com.j_spaces.core.client.EntryVersionConflictException;

import org.springframework.dao.OptimisticLockingFailureException;

/**
 * This exception is thrown when update/take/change/clear operation is rejected as a result of
 * optimistic locking version conflict. Wraps {@link com.j_spaces.core.client.EntryVersionConflictException}.
 *
 * @author kimchy
 */
public class SpaceOptimisticLockingFailureException extends OptimisticLockingFailureException {

    private static final long serialVersionUID = 2963324400567929826L;

    private EntryVersionConflictException e;

    public SpaceOptimisticLockingFailureException(EntryVersionConflictException e) {
        super(e.getMessage(), e);
        this.e = e;
    }

    /**
     * Returns the entry UID.
     *
     * @see com.j_spaces.core.client.EntryVersionConflictException#getUID()
     */
    public String getUID() {
        return e.getUID();
    }

    /**
     * Returns the entry Space Version ID.
     *
     * @see com.j_spaces.core.client.EntryVersionConflictException#getSpaceVersionID()
     */
    public int getSpaceVersionID() {
        return e.getSpaceVersionID();
    }

    /**
     * Returns the entry client Version ID
     *
     * @see com.j_spaces.core.client.EntryVersionConflictException#getClientVersionID()
     */
    public int getClientVersionID() {
        return e.getClientVersionID();
    }

    /**
     * Return the space operation caused the conflict Take or Update
     *
     * @see com.j_spaces.core.client.EntryVersionConflictException#getOperation()
     */
    public String getOperation() {
        return e.getOperation();
    }

}
