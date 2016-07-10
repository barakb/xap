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

import org.springframework.dao.DataRetrievalFailureException;

/**
 * This exception is thrown when <code>update</code>, <code>readIfExist</code> or
 * <code>takeIfExist</code> operations are rejected. The entry specified by the UID is not in the
 * space - it was not found or has been deleted. Wraps {@link com.j_spaces.core.client.EntryNotInSpaceException}.
 *
 * @author kimchy
 */
public class EntryNotInSpaceException extends DataRetrievalFailureException {

    private static final long serialVersionUID = 1654923353943041796L;

    private com.j_spaces.core.client.EntryNotInSpaceException e;

    public EntryNotInSpaceException(com.j_spaces.core.client.EntryNotInSpaceException e) {
        super(e.getMessage(), e);
        this.e = e;
    }

    /**
     * Returns Entry UID.
     *
     * @return unique ID of the entry that caused this exception
     * @see com.j_spaces.core.client.EntryNotInSpaceException#getUID()
     */
    public String getUID() {
        return e.getUID();
    }

    /**
     * Check if deleted in the same transaction.
     *
     * @return <code>true</code> if deleted by the same transaction
     * @see com.j_spaces.core.client.EntryNotInSpaceException#isDeletedByOwnTxn()
     */
    public boolean isDeletedByOwnTxn() {
        return e.isDeletedByOwnTxn();
    }
}
