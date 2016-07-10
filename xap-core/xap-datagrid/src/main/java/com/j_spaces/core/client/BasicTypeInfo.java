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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.Serializable;

/**
 * This class provides Entry Class information. <br> Here is a simple usage of the BasicTypeInfo
 * class:
 * <pre>
 * IJSpace space = (IJSpace) SpaceFinder.find(spaceURL);
 * com.j_spaces.core.client.BasicTypeInfo classInfo = ((IRemoteJSpaceAdmin)space.getAdmin()).getClassTypeInfo(className);
 * String[] fieldsNames = classInfo.getFieldsNames();
 * String[] fieldsTypes = classInfo.getFieldsTypes();
 * </pre>
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see com.j_spaces.core.admin.IRemoteJSpaceAdmin
 * @deprecated Since 8.0 - Use {@link SpaceTypeDescriptor} instead.
 **/
@Deprecated

public class BasicTypeInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * The type class name.
     */
    final public String m_ClassName;
    /**
     * The type code base.
     */
    public String m_CodeBase;
    /**
     * The type super classes names.
     */
    final public String[] m_SuperClasses;
    /**
     * The type fields names.
     */
    final public String[] m_FieldsNames;
    /**
     * The type fields types correlated to <code>m_FieldsNames</code>.
     */
    final public String[] m_FieldsTypes;
    /**
     * The type fields index indicators correlated to <code>m_FieldsNames</code>.
     */
    public boolean[] m_IndexedFields;
    /**
     * Indicates if the type is et to FIFO.
     */
    public boolean m_IsFifo;
    /**
     * Indicates if the type is et to Replicatable.
     */
    public boolean m_IsReplicatable;

    /**
     * Internal use only.
     */
    private final int _checksum;

    final private boolean _isExtendedIndexing;

    /**
     * one of the {@link ObjectFormat} formats
     */
    private short _objectFormat;


    public BasicTypeInfo(ITypeDesc typeDesc) {
        m_ClassName = typeDesc.getTypeName();
        m_CodeBase = typeDesc.getCodeBase();
        m_SuperClasses = typeDesc.getSuperClassesNames();

        m_FieldsNames = typeDesc.getPropertiesNames();
        m_FieldsTypes = typeDesc.getPropertiesTypes();
        m_IndexedFields = typeDesc.getPropertiesIndexTypes();

        m_IsFifo = typeDesc.isFifoSupported();
        m_IsReplicatable = typeDesc.isReplicable();

        _checksum = typeDesc.getChecksum();
        _isExtendedIndexing = false;
    }

    /**
     * Returns Entry Class Name.
     *
     * @return Returns Entry Class Name.
     **/
    public String getClassName() {
        return m_ClassName;
    }

    /**
     * Returns Entry Class Codebase.
     *
     * @return Returns Entry Class Codebase.
     */
    public String getCodeBase() {
        return m_CodeBase;
    }

    /**
     * Returns Entry Class Fields Names array.
     *
     * @return Returns Entry Class Fields Names array.
     **/
    public String[] getFieldsNames() {
        return m_FieldsNames;
    }

    /**
     * Returns Entry Class Fields Types array.
     *
     * @return Returns Entry Class Fields Types array.
     */
    public String[] getFieldsTypes() {
        return m_FieldsTypes;
    }

    /**
     * Returns Entry indexed Fields.
     *
     * @return Returns Entry indexed Fields.
     **/
    public boolean[] getIndexedFields() {
        return m_IndexedFields;
    }

    /**
     * Returns Entry Super Classes array.
     *
     * @return Returns Entry Super Classes array.
     **/
    public String[] getSuperClasses() {
        return m_SuperClasses;
    }

    /**
     * Returns <code>true</code> if FIFO enabled, otherwise <code>false</code>.
     *
     * @return Returns <code>true</code> if FIFO enabled, otherwise <code>false</code>.
     **/
    public boolean isFifo() {
        return m_IsFifo;
    }

    /**
     * Returns <code>true</code> if this class Replicatable, otherwise <code>false</code>.
     *
     * @return Returns <code>true</code> if this class Replicatable, otherwise <code>false</code>.
     **/
    public boolean isReplicatable() {
        return m_IsReplicatable;
    }

    @Deprecated
    public int getSuperClassesChecksum() {
        return _checksum;
    }

    @Deprecated
    public int getFieldsNamesChecksum() {
        return _checksum;
    }

    @Deprecated
    public int getFieldsTypesChecksum() {
        return _checksum;
    }

    @Deprecated
    public int getPropertiesChecksum() {
        return _checksum;
    }

    public int getChecksum() {
        return _checksum;
    }

    /**
     * Prints out to the standard output the BasicTypeInfo state <b>Should be used for debug
     * purposes.</b>
     */
    public void dump() {
        StringBuilder sb = new StringBuilder("DUMP : m_ClassName ");
        sb.append("ClassName: ").append(m_ClassName);
        sb.append("\nCodeBase: ").append(m_CodeBase);
        sb.append("\nm_FieldsNames: ").append(arrayToString("m_FieldsNames", m_FieldsNames));
        sb.append("\nm_SuperClasses: ").append(arrayToString("m_SuperClasses", m_SuperClasses));
        sb.append("\nm_FieldsTypes: ").append(arrayToString("m_FieldsTypes", m_FieldsTypes));
        sb.append("\nm_IsFifo: ").append(m_IsFifo);
        sb.append("\nm_IsReplicatable: ").append(m_IsReplicatable);
        sb.append("\nm_IndexedFields: ").append(booleanToString("m_IndexedFields", m_IndexedFields));

        System.out.println(sb);
    }

    private String booleanToString(String descArr, boolean[] strArr) {
        StringBuilder sb = new StringBuilder();
        if (strArr == null) {
            sb.append("==========").append(descArr).append("========== NULL");
            return sb.toString();
        }

        sb.append("==========").append(descArr).append("==========");
        for (int i = 0; i < strArr.length; i++)
            sb.append('\n').append(strArr[i]);

        sb.append("\n===============");
        return sb.toString();
    }

    private String arrayToString(String descArr, Object[] strArr) {
        StringBuilder sb = new StringBuilder();
        if (strArr == null) {
            sb.append("==========").append(descArr).append("========== NULL");
            return sb.toString();
        }

        sb.append("==========").append(descArr).append("==========");
        for (int i = 0; i < strArr.length; i++)
            sb.append('\n').append(strArr[i]);

        sb.append("\n===============");
        return sb.toString();
    }

    /**
     * Indicator if this class has extended indexing turned on.
     *
     * @return <code>true</code> if class participates in extended indexing; <code>false</code>
     * otherwise.
     */
    public boolean isExtendedIndexing() {
        return _isExtendedIndexing;
    }
}
