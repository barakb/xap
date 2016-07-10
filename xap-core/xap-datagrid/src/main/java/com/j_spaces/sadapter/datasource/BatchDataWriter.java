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

/**
 *
 */
package com.j_spaces.sadapter.datasource;

import com.gigaspaces.datasource.DataSourceException;

import java.util.HashMap;
import java.util.LinkedList;


/**
 * Handles batch writes according to the data type and the underlying data source
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class BatchDataWriter

{
    // Hold separate data structures for each entry type
    // Usually only one of them holds the entries
    protected HashMap<Object, Object> _map = new HashMap<Object, Object>();
    protected LinkedList _list = new LinkedList();

    protected DataStorage<Object> _storage;

    /**
     * @param storage
     */
    public BatchDataWriter(DataStorage<Object> storage) {
        super();
        _storage = storage;
    }


    /* (non-Javadoc)
     * @see com.j_spaces.sadapter.datasource.visitors.DataSourceVisitor#visit(java.lang.Object)
     */
    public void add(BulkDataItem entry)
            throws DataSourceException {


        if (_storage.isBulkDataPersister()) {
            _list.add(entry);
        } else if (_storage.isDataPersister()) {
            _list.add(entry.toObject());
        } else {
            throw new UnsupportedOperationException("Failed to insert new object - operation not supported by the data source.");
        }

    }

    /* (non-Javadoc)
     * @see com.j_spaces.sadapter.datasource.visitors.BatchDataVisitor#executeBatch()
     */
    public void executeBatch() throws DataSourceException {

        _map.clear();

        if (!_list.isEmpty()) {
            if (_storage.isBulkDataPersister()) {
                _storage.executeBulk(_list);
            } else {
                _storage.writeBatch(_list);
            }
        }
        _list.clear();


    }
}
