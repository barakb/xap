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


package com.gigaspaces.datasource;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

import java.util.Map;

/**
 * BulkItem defines a single item in a bulk.
 *
 * @author anna
 * @see BulkDataPersister
 * @since 6.0
 * @deprecated since 9.5 - use {@link SpaceSynchronizationEndpoint} and {@link DataSyncOperation}
 * instead.
 */
@Deprecated
public interface BulkItem extends DataSyncOperation {

    /**
     * Remove operation
     */
    public final static short REMOVE = 1;
    /**
     * Update operation
     */
    public final static short UPDATE = 2;
    /**
     * Write operation
     */
    public final static short WRITE = 3;
    /**
     * Partial update operation
     */
    public final static short PARTIAL_UPDATE = 4;
    /**
     * Represents Change operation but it is not supported using the BulkItem API, use {@link
     * SpaceSynchronizationEndpoint} instead.
     */
    public final static short CHANGE = 5;


    /**
     * Return the data item
     *
     * @return data item
     */
    Object getItem();

    /**
     * Return the operation to execute
     *
     * @return operation type {@link #REMOVE}/{@link #UPDATE}/{@link #WRITE}/{@link #PARTIAL_UPDATE}
     */
    short getOperation();

    /**
     * @return the name of the type/class
     */
    String getTypeName();

    /**
     * @return the name of the id property
     */
    String getIdPropertyName();

    /**
     * @return the value of the id property
     */
    Object getIdPropertyValue();

    /**
     * @return a map of the object properties. The keys are the names of the properties and their
     * values is the object values. In case of a partial update - only the updated values are
     * returned.s
     */
    Map<String, Object> getItemValues();
}
