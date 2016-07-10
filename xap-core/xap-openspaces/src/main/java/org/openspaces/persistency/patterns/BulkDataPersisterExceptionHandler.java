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

import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;

import java.util.List;

/**
 * An exception handler that delegates {@link com.gigaspaces.datasource.BulkDataPersister} execution
 * and calls the provided {@link org.openspaces.persistency.patterns.ExceptionHandler} in case of
 * exceptions.
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link SpaceSynchronizationEndpointExceptionHandler} instead.
 */
@Deprecated
public class BulkDataPersisterExceptionHandler extends AbstractManagedDataSourceDelegator implements BulkDataPersister {

    protected final ExceptionHandler exceptionHandler;

    public BulkDataPersisterExceptionHandler(ManagedDataSource dataSource, ExceptionHandler exceptionHandler) {
        super(dataSource);
        if (!(dataSource instanceof BulkDataPersister)) {
            throw new IllegalArgumentException("data source [" + dataSource + "] must implement BulkDataPersister");
        }
        this.exceptionHandler = exceptionHandler;
    }

    public void executeBulk(List<BulkItem> bulkItems) throws DataSourceException {
        try {
            ((BulkDataPersister) dataSource).executeBulk(bulkItems);
        } catch (Exception e) {
            exceptionHandler.onException(e, bulkItems);
        }
    }
}
