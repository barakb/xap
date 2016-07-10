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

import com.j_spaces.core.exception.internal.EngineInternalSpaceException;

/**
 * This exception is thrown when a field-value cannot be cloned. Cloning (either by using clone()
 * method if supported or by marshaling and unmarshaling is done in order to protect space data
 * structures for enbedded References fields changing a duplicate value for unique index
 *
 * @author Yechiel
 * @version 8.04
 **/
@com.gigaspaces.api.InternalApi
public class FieldValueCloningErrorException
        extends EngineInternalSpaceException {
    private static final long serialVersionUID = 3403206627646407089L;

    final private String m_UID;
    final private String m_ClassName;
    final private String _field_ClassName;
    final private Object _value;
    final private Exception _cause;

    /**
     * Constructor.
     *
     * @param uid             the entry UID
     * @param classname       entry class name
     * @param field_className field class name
     */
    public FieldValueCloningErrorException(String uid, String classname, String field_className, Object value, Exception cause) {
        super("Entry UID=" + uid + " class=" + classname + " field-class=" + field_className + " value " + value.toString() +
                " cloning failed cause= " + cause);

        m_UID = uid;
        m_ClassName = classname;
        _value = value;
        _cause = cause;
        _field_ClassName = field_className;
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
     * @since 5.0
     */
    public String getClassName() {
        return m_ClassName;
    }

    public String getFieldClassName() {
        return _field_ClassName;
    }

    public Object getValue() {
        return _value;
    }

    public Exception getCause() {
        return _cause;
    }


}
