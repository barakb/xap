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

/**
 * <pre>
 * {@link ExternalDataSource} is an interface for space external data source.
 * This interface provides all the data source functionality supported by space.
 *
 * Supported operations:
 * 1. read - {@link DataProvider}
 * 2. write,update,remove - {@link DataPersister}
 * 3. batch write,update, remove - {@link DataPersister}
 * 4. complex queries - {@link SQLDataProvider}
 * 5. bulk operations - used in transactions and mirror spaces - {@link BulkDataPersister}
 *
 *  </pre>
 *
 * @author anna
 * @since 6.0
 * @deprecated since 9.5 - use {@link SpaceDataSource} and {@link SpaceSynchronizationEndpoint}
 * instead.
 */
@Deprecated
public interface ExternalDataSource<T>
        extends DataProvider<T>, DataPersister<T>, SQLDataProvider<T>, BulkDataPersister {

}
