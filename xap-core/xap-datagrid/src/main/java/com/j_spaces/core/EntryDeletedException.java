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

package com.j_spaces.core;

/**
 * This exception is thrown when the updated entry does not exist in space in optimistic locking
 * scenarios.
 *
 * @author Yechel Fefer
 * @version 3.2
 **/
@com.gigaspaces.api.InternalApi
public class EntryDeletedException extends Exception {
    private static final long serialVersionUID = 5387150632460999881L;

    private String uid;
    private boolean _deletedByOwnTxn;

    public EntryDeletedException() {
        super();
    }

    public EntryDeletedException(String uid) {
        super("Entry Deleted: " + uid);

        this.uid = uid;
    }

    public EntryDeletedException(boolean deletedByOwnTxn) {
        super();
        _deletedByOwnTxn = deletedByOwnTxn;
    }

    /**
     * Return entry UID
     **/
    public String getUID() {
        return uid;
    }

    /**
     * @return <tt>true</tt> if entry is marked deleted by own transaction and not-yet committed.
     */
    public boolean deletedByOwnTxn() {
        return _deletedByOwnTxn;
    }

    /**
     * Override the method to avoid expensive stack build and synchronization, since no one uses it
     * anyway.
     */
    @Override
    public Throwable fillInStackTrace() {
        return null;
    }

}