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

/**
 * This exception is thrown when write operation is rejected when the entry (or another with same
 * UID) is already in space.
 *
 * @author Yechiel
 * @version 3.0
 **/

public class EntryAlreadyInSpaceException
        extends RuntimeException {
    private static final long serialVersionUID = -8345114876101707233L;

    final private String m_UID;
    final private String m_ClassName;

    /**
     * Constructor.
     *
     * @param uid       the rejected entry UID
     * @param classname the rejected entry class name
     */
    public EntryAlreadyInSpaceException(String uid, String classname) {
        super("Entry UID=" + uid + " class=" + classname +
                " rejected - an entry with the same UID already in space.");

        m_UID = uid;
        m_ClassName = classname;
    }


    /**
     * Returns the rejected entry UID.
     *
     * @return the rejected entry UID
     */
    public String getUID() {
        return m_UID;
    }

    /**
     * Returns the rejected entry class name.
     *
     * @return the rejected entry class name
     * @since 5.0
     */
    public String getClassName() {
        return m_ClassName;
    }


}