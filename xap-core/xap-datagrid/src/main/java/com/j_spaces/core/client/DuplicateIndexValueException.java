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

import com.j_spaces.core.cache.ICompoundIndexValueHolder;


/**
 * This exception is thrown when a write or update operation encounters a duplicate value for unique
 * index
 *
 * @author Yechiel
 * @version 7.0
 **/
@com.gigaspaces.api.InternalApi
public class DuplicateIndexValueException extends RuntimeException implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    final private String m_UID;
    final private String m_ClassName;
    final private Object _value;
    final private String m_OtherUid;
    final private String _indexName;

    public DuplicateIndexValueException(String uid, String classname, String indexName, Object value, String otherUid) {
        super("Entry UID=" + uid + " class=" + classname + " index=" + indexName + " value=" + value.toString() +
                " rejected: an entry with the same index value already in space. otheruid=" + otherUid);

        m_UID = uid;
        m_ClassName = classname;
        _value = (value instanceof ICompoundIndexValueHolder) ? ((ICompoundIndexValueHolder) value).getValues() : value;
        m_OtherUid = otherUid;
        _indexName = indexName;
    }


    /**
     * Returns the rejected entry UID.
     */
    public String getUID() {
        return m_UID;
    }

    /**
     * Returns the rejected entry class name.
     *
     * @since 5.0
     */
    public String getClassName() {
        return m_ClassName;
    }

    public Object getValue() {
        return _value;
    }

    public String getIndexName() {
        return _indexName;
    }
}
