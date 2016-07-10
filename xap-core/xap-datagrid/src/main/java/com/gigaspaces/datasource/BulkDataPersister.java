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
 * {@link BulkDataPersister} is responsible for  executing bulk operations<br>
 *
 *
 * This interface should be implemented when using mirror space <br> or space transactions.
 *
 * @author anna
 * @since 6.0
 * @deprecated since 9.5 - use {@link SpaceSynchronizationEndpoint} instead.
 */
@Deprecated
public interface BulkDataPersister

{

    /**
     * Execute given bulk of operations.<br> Each {@link BulkItem} contains one of the following
     * operation -<br><br>
     *
     * WRITE - given object should be inserted to the data store,<br> UPDATE - given object should
     * be updated in the data store,<br> REMOVE - given object should be deleted from the data
     * store<br> <br> If the implementation uses transactions,<br> all the bulk operations must be
     * executed in one  transaction.<br>
     *
     * @param bulk list of data object and the operation to apply on that object
     * @throws DataSourceException when the data store fails to persist the given list of objects
     */
    public void executeBulk(List<BulkItem> bulk)
            throws DataSourceException;

}
