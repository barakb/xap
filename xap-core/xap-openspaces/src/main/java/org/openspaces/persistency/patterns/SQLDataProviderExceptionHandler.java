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

package org.openspaces.persistency.patterns;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SQLDataProvider;
import com.j_spaces.core.client.SQLQuery;

/**
 * @author kimchy
 * @deprecated since 9.5 - use {@link SpaceDataSourceExceptionHandler} instead.
 */
@Deprecated
public class SQLDataProviderExceptionHandler extends BulkDataPersisterExceptionHandler implements SQLDataProvider {

    public SQLDataProviderExceptionHandler(ManagedDataSource dataSource, ExceptionHandler exceptionHandler) {
        super(dataSource, exceptionHandler);
        if (!(dataSource instanceof SQLDataProvider)) {
            throw new IllegalArgumentException("data source [" + dataSource + "] must implement SQLDataProvider");
        }
    }

    public DataIterator iterator(SQLQuery sqlQuery) throws DataSourceException {
        try {
            return ((SQLDataProvider) dataSource).iterator(sqlQuery);
        } catch (Exception e) {
            exceptionHandler.onException(e, sqlQuery);
            return null;
        }
    }
}
