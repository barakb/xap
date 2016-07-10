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
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

/**
 * An exception handler that delegates {@link com.gigaspaces.datasource.SpaceDataSource} execution
 * and calls the provided {@link org.openspaces.persistency.patterns.PersistencyExceptionHandler} in
 * case of exceptions on query methods.
 *
 * @author eitany
 * @since 9.5
 */
public class SpaceDataSourceExceptionHandler extends SpaceDataSource {

    private final SpaceDataSource dataSource;

    private final PersistencyExceptionHandler exceptionHandler;

    public SpaceDataSourceExceptionHandler(SpaceDataSource dataSource, PersistencyExceptionHandler exceptionHandler) {
        this.dataSource = dataSource;
        this.exceptionHandler = exceptionHandler;
    }

    public DataIterator<Object> initialDataLoad() {
        return dataSource.initialDataLoad();
    }

    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        return dataSource.initialMetadataLoad();
    }

    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        try {
            return dataSource.getDataIterator(query);
        } catch (Exception e) {
            exceptionHandler.onException(e, query);
            return null;
        }
    }

    public Object getById(DataSourceIdQuery idQuery) {
        try {
            return dataSource.getById(idQuery);
        } catch (Exception e) {
            exceptionHandler.onException(e, idQuery);
            return null;
        }
    }

    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
        try {
            return dataSource.getDataIteratorByIds(idsQuery);
        } catch (Exception e) {
            exceptionHandler.onException(e, idsQuery);
            return null;
        }
    }

    public boolean supportsInheritance() {
        return dataSource.supportsInheritance();
    }

}
