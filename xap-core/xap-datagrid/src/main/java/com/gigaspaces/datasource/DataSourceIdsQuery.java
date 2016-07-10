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

import com.gigaspaces.metadata.SpaceTypeDescriptor;

/**
 * Represents a query by IDs executed against a {@link SpaceDataSource} implementation.
 *
 * @author Dan Kilman
 * @since 9.5
 */
public interface DataSourceIdsQuery {

    /**
     * @return The {@link SpaceTypeDescriptor} representing the type this query is for.
     */
    SpaceTypeDescriptor getTypeDescriptor();

    /**
     * @return The IDs of the entries to get.
     */
    Object[] getIds();

    /**
     * @return Whether this query has an SQL query representation.
     */
    public boolean supportsAsSQLQuery();

    /**
     * Gets the prepared SQL representation of this ids query.<br/> {@link
     * DataSourceQuery#supportsAsSQLQuery()} return value should be checked before calling this
     * method otherwise an {@link UnsupportedOperationException} will be thrown if the operation is
     * not supported.
     *
     * @return Prepared SQL representation this query.
     */
    public DataSourceSQLQuery getAsSQLQuery();

}
