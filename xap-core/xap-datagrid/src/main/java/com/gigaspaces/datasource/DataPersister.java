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

import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

import java.util.List;

/**
 * DataPersister is responsible for the persistency of  the space data in external  data source.
 * <br><br>
 *
 * This interface should be implemented for non-transactional single operations applications.<br>
 *
 * Single operations - write,update and remove are used for objects with UID. <br>
 *
 * For batch operations and transactions use the {@link BulkDataPersister}.
 *
 * @author anna
 * @since 6.0
 * @deprecated since 9.5 - use {@link SpaceSynchronizationEndpoint} instead.
 */
@Deprecated
public interface DataPersister<T> extends DataProvider<T> {

    /**
     * Write given new object to the data store
     */
    void write(T object) throws DataSourceException;

    /**
     * Update the given object in the data store
     */
    void update(T object) throws DataSourceException;

    /**
     * Remove the given object from the data store<br>
     */
    void remove(T object) throws DataSourceException;


    /**
     * Write  given new objects to the data store.<br><br>
     *
     * If the implementation uses transactions,<br> all the objects must be written in one
     * transaction.<br>
     *
     * @deprecated for batch operations use {@link BulkDataPersister}
     */
    @Deprecated
    void writeBatch(List<T> objects)
            throws DataSourceException;


}
