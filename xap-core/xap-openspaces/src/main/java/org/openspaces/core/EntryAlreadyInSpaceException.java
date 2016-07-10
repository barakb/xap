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

import org.springframework.dao.DataIntegrityViolationException;

/**
 * This exception is thrown when write operation is rejected when the entry (or another with same
 * UID) is already in space. Wraps {@link com.j_spaces.core.client.EntryAlreadyInSpaceException}.
 *
 * @author kimchy
 */
public class EntryAlreadyInSpaceException extends DataIntegrityViolationException {

    private static final long serialVersionUID = -8553568598873283849L;

    private com.j_spaces.core.client.EntryAlreadyInSpaceException e;

    public EntryAlreadyInSpaceException(com.j_spaces.core.client.EntryAlreadyInSpaceException e) {
        super(e.getMessage(), e);
        this.e = e;
    }

    /**
     * Returns the rejected entry UID.
     *
     * @see com.j_spaces.core.client.EntryAlreadyInSpaceException#getUID()
     */
    public String getUID() {
        return e.getUID();
    }

    /**
     * Returns the rejected entry class name.
     *
     * @see com.j_spaces.core.client.EntryAlreadyInSpaceException#getClassName()
     */
    public String getClassName() {
        return e.getClassName();
    }
}
