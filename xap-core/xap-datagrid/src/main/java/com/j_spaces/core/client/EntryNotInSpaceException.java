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


package com.j_spaces.core.client;

import com.j_spaces.core.DetailedUnusableEntryException;

/**
 * This exception is thrown when update operation is rejected. The entry specified by the UID is not
 * in the space - it was not found or has been deleted
 *
 * @author Yechiel
 * @version 3.0
 **/


public class EntryNotInSpaceException extends DetailedUnusableEntryException {
    private static final long serialVersionUID = 5273517108969045717L;

    private String m_UID;
    private boolean m_DeletedByOwnTxn;

    /**
     * Constructor.
     *
     * @param uid            unique ID of the entry
     * @param spaceName      the name of the space that throws this exception
     * @param deletedByOwnTx <code>true</code> if deleted in the same transaction
     */
    public EntryNotInSpaceException(String uid, String spaceName, boolean deletedByOwnTx) {
        super("Entry UID " + uid + " is not in space " + spaceName);

        m_UID = uid;
        m_DeletedByOwnTxn = deletedByOwnTx;
    }

    /**
     * Returns Entry UID.
     *
     * @return unique ID of the entry that caused this exception
     */
    public String getUID() {
        return m_UID;
    }

    /**
     * Check if deleted in the same transaction.
     *
     * @return <code>true</code> if deleted by the same transaction
     */
    public boolean isDeletedByOwnTxn() {
        return m_DeletedByOwnTxn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage() {
        if (!m_DeletedByOwnTxn)
            return super.getMessage();

        return "Conflicting operation: Entry is pending deletion under this transaction.\n\t"
                + super.getMessage();
    }
}