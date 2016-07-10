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
 * @author Yechiel Fefer
 * @version 1.0
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SequenceNumberException extends RuntimeException implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    final private String m_UID;
    final private String m_ClassName;

    public SequenceNumberException(String uid, String classname, String errorDescription) {
        super("sequence-number problem Entry UID=" + uid + " class=" + classname + " " + errorDescription);

        m_UID = uid;
        m_ClassName = classname;
    }

    public SequenceNumberException(String classname, String errorDescription) {
        super("sequence-number problem class=" + classname + " " + errorDescription);

        m_UID = null;
        m_ClassName = classname;
    }

    /**
     * Returns the rejected entry UID.
     */
    public String getUID() {
        return m_UID;
    }

    /**
     * Returns the rejected entry class name.
     */
    public String getClassName() {
        return m_ClassName;
    }


}
