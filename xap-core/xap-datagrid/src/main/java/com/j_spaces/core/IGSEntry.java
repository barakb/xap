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

import net.jini.core.entry.UnusableEntryException;

import java.io.Serializable;
import java.util.Map;

/**
 * This class represents an {@link net.jini.core.entry.Entry Entry} in a GigaSpace. Each instance of
 * this class contains a reference to the Entry value plus any other necessary info about the entry;
 * including its class name, field types, and field values (could be in a {@link
 * java.rmi.MarshalledObject MarshalledObject} or {@link com.gigaspaces.internal.io.MarshObject
 * MarshObject} form).
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
public interface IGSEntry extends Serializable {
    /**
     * Returns a <tt>Map.Entry</tt> (key-value pair) representation of this entity. Relevant when
     * interacting with the Space using Map API; otherwise a <code>null</code> is returned.
     *
     * @return a <tt>Map.Entry</tt> representation of this entity; <code>null</code> if no
     * representation.
     * @see java.util.Map.Entry
     */
    Map.Entry getMapEntry();

    /**
     * Entry UID.
     *
     * @return Returns the Entry UID
     */
    String getUID();

    /**
     * Entry class Name.
     *
     * @return Returns the Entry class Name.
     */
    String getClassName();

    /**
     * Entry Super Classes Names.
     *
     * @return Returns the Entry Super Classes Names.
     */
    String[] getSuperClassesNames();

    /**
     * Entry-class codebase.
     *
     * @return returns the codebase of this entry.
     */
    String getCodebase();

    /**
     * Entry fields names.
     *
     * @return Returns the entry fields names.
     */
    String[] getFieldsNames();

    /**
     * Entry Fields Types.
     *
     * @return Returns The entry Fields Types
     */
    String[] getFieldsTypes();

    /**
     * Entry field Values.
     *
     * @return Returns the entry field Values
     */
    Object[] getFieldsValues();

    /**
     * Indexed fields array indication.
     *
     * @return Returns the indexed fields array indication
     */
    boolean[] getIndexIndicators();

    /**
     * The field name representing the primary key. Usually the field name of the first indexed
     * field (can be null).
     *
     * @return field name of the primary key.
     */
    String getPrimaryKeyName();

    /**
     * If true operations will be done using FIFO ordering when multiple match found.
     *
     * @return <code>true</code> if FIFO ordering is used
     */
    boolean isFifo();

    /**
     * Checks entry is transient even if space is persistent.
     *
     * @return <code>true</code> if entry is transient
     */
    boolean isTransient();

    /**
     * Check if replicatable, applicable only when used with partial replication.
     *
     * @return <code>true</code> if this entry is replicatable.
     * @see com.j_spaces.core.client.IReplicatable
     */
    boolean isReplicatable();

    /**
     * Entry time to live.
     *
     * @return the Entry's time to live in milliseconds.
     * @see net.jini.core.lease.Lease
     */
    long getTimeToLive();

    /**
     * Ascending version of this Entry
     *
     * @return the version of this entry.
     */
    int getVersion();

    /**
     * Return the the field position in the {@link #getFieldsValues() FieldsValues} array.
     *
     * @param fieldName name of the field (e.g. {@link java.lang.reflect.Field#getName()
     *                  Field.getName()}.
     * @return the field position in the field values array returned by {{@link #getFieldsValues()}.
     */
    int getFieldPosition(String fieldName);

    /**
     * Retrieves the given field value.
     *
     * @param fieldName name of the field (e.g. {@link java.lang.reflect.Field#getName()
     *                  Field.getName()}.
     * @return the field value.
     * @throws IllegalArgumentException if field name is not avaliable
     * @throws IllegalStateException    if field values array was not properly set
     */
    Object getFieldValue(String fieldName) throws IllegalArgumentException, IllegalStateException;

    /**
     * Retrieves the given field value by position.
     *
     * @param position the field position.
     * @return the field value.
     * @throws IllegalArgumentException if field name is not avaliable
     * @throws IllegalStateException    if field values array was not properly set
     */
    Object getFieldValue(int position) throws IllegalArgumentException, IllegalStateException;

    /**
     * Associates the specified value with the specified field.
     *
     * @param fieldName the field name.
     * @param value     value to be associated with the specified field.
     * @return the value that was associated with the specified field.
     * @throws IllegalArgumentException if field name is not avaliable
     * @throws IllegalStateException    if field values array was not properly set
     */
    Object setFieldValue(String fieldName, Object value) throws IllegalArgumentException, IllegalStateException;

    /**
     * Associates the specified value with the specified field position.
     *
     * @param position the field position.
     * @param value    value to be associated with the specified field.
     * @return the value that was associated with the specified field.
     * @throws IllegalArgumentException if field name is not avaliable
     * @throws IllegalStateException    if field values array was not properly set
     */
    Object setFieldValue(int position, Object value) throws IllegalArgumentException, IllegalStateException;

    /**
     * Retries the given field its type class name.
     *
     * @param fieldName the field name.
     * @return the field type class name.
     * @throws IllegalArgumentException if field name is not avaliable
     * @throws IllegalStateException    if field types array was not properly set
     */
    String getFieldType(String fieldName) throws IllegalArgumentException, IllegalStateException;

    /**
     * Checks if the given field is indexed.
     *
     * @param fieldName the field name.
     * @return <code>true</code> if the the field is indexed.
     * @throws IllegalArgumentException if field name is not avaliable
     * @throws IllegalStateException    if field indexes array was not properly set
     */
    boolean isIndexedField(String fieldName) throws IllegalArgumentException, IllegalStateException;

    /**
     * Converts to object. <BR> Notice: The returned object is a transformation of this IGSEntry and
     * each change in entry fields should be using {@link #setFieldValue(int, Object)} or {@link
     * #setFieldValue(String, Object)}.
     *
     * @param space Space proxy.
     * @return Returns converted Object.
     * @throws UnusableEntryException One or more fields in the entry cannot be deserialized, or the
     *                                class for the entry type itself cannot be deserialized.
     **/
    public Object getObject(IJSpace space) throws UnusableEntryException;

    String getRoutingFieldName();
}
