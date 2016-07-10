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


/**
 * DataProvider is responsible for retrieving  space data from external  data source. <br><br> This
 * interfaces should be implemented for simple space access.<br><br>
 *
 * DataProvider is used by the space in the following scenarios:<br>
 *
 * - reads by UID. <br> - empty template. <br> - non-abstract class without extended match
 * codes.<br><br>
 *
 * Any other scenario requires an implementation of the {@link SQLDataProvider}. Note: If {@link
 * SQLDataProvider} is implemented, the space will use it for all the read operations instead of the
 * {@link DataProvider}.
 *
 * Implement this interface only if you don't intend to use SQLQuery at all.
 *
 * @author anna
 * @see SQLDataProvider
 * @since 6.0
 * @deprecated since 9.5 - use {@link SpaceDataSource} instead.
 */
@Deprecated
public interface DataProvider<T> extends ManagedDataSource<T> {

    /**
     * Read one object that matches the given template. <br> Used by the space for read templates
     * with UID.<br>
     *
     * @return matched object or null
     */
    public abstract T read(T template)
            throws DataSourceException;

    /**
     * Create an iterator over all objects that match the given template.<br> Note: null value can
     * be passed - in case of a null template or at initial space load. If {@link SQLDataProvider}
     * interface is also implemented - the space will use {@link SQLDataProvider}.iterator instead.
     *
     * @return a {@link DataIterator} or null if no data was found that match the given template
     */
    public abstract DataIterator<T> iterator(T template)
            throws DataSourceException;


}