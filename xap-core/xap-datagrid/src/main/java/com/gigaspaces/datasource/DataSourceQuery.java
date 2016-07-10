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

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;


/**
 * Represents a query executed against a {@link SpaceDataSource} implementation.
 *
 * @author eitany
 * @author Idan Moyal
 * @since 9.1.1
 */
public interface DataSourceQuery {
    /**
     * Gets the prepared SQL representation of this query.<br/> {@link
     * DataSourceQuery#supportsAsSQLQuery()} return value should be checked before calling this
     * method otherwise an {@link UnsupportedOperationException} will be thrown if the operation is
     * not supported.
     *
     * @return Prepared SQL representation this query.
     */
    DataSourceSQLQuery getAsSQLQuery();

    /**
     * Gets an object representation of this query.<br/> {@link DataSourceQuery#supportsTemplateAsObject()}
     * return value should be checked before calling this method otherwise an {@link
     * UnsupportedOperationException} will be thrown if the operation is not supported.
     *
     * @return Object representation of this query.
     */
    Object getTemplateAsObject();


    /**
     * Gets a {@link SpaceDocument} representation of this query.<br/> {@link
     * DataSourceQuery#supportsTemplateAsDocument()} return value should be checked before calling
     * this method otherwise an {@link UnsupportedOperationException} will be thrown if the
     * operation is not supported.
     *
     * @return {@link SpaceDocument} representation of this query.
     */
    SpaceDocument getTemplateAsDocument();

    /**
     * @return Whether this query has an SQL query representation.
     */
    boolean supportsAsSQLQuery();

    /**
     * @return Whether this query has an Object representation.
     */
    boolean supportsTemplateAsObject();

    /**
     * @return Whether this query has a {@link SpaceDocument} representation.
     */
    boolean supportsTemplateAsDocument();

    /**
     * @return The {@link SpaceTypeDescriptor} representing the type this query is for.
     */
    SpaceTypeDescriptor getTypeDescriptor();

    /**
     * The value returned from this method can serve as a hint for the data source implementation
     * which indicates the batch size to be used in {@link SpaceDataSource#getDataIterator(DataSourceQuery)}
     * method implementation (if such data source supports batching). One should use this value with
     * care since it might return {@link Integer#MAX_VALUE} if the batch operation executed had a
     * request for unlimited results.
     *
     * @return Batch size hint.
     */
    int getBatchSize();

}
