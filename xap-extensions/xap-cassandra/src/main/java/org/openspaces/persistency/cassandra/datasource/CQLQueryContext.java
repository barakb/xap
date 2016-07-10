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

package org.openspaces.persistency.cassandra.datasource;

import java.util.Arrays;
import java.util.Map;

/**
 * CQL query context for holding the properies map if this is a template based query or the {@link
 * String} query and the parameters array if this is a complex query.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CQLQueryContext {

    private final Map<String, Object> properties;
    private final String sqlQuery;
    private final Object[] parameters;

    public CQLQueryContext(Map<String, Object> properties, String sqlQuery, Object[] parameters) {
        this.properties = properties;
        this.sqlQuery = sqlQuery;
        this.parameters = parameters;
    }

    public boolean hasProperties() {
        return properties != null;
    }

    /**
     * @return The properties matching the query if this is a template based query, null otherwise.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * @return The complex sql query if it one. null if this is a template based query.
     */
    public String getSqlQuery() {
        return sqlQuery;
    }

    /**
     * @return The complex sql query parameters if it one. null if this is a template based query.
     */
    public Object[] getParameters() {
        return parameters;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return " [properties=" + properties + ", sqlQuery=" + sqlQuery + ", parameters="
                + Arrays.toString(parameters) + "]";
    }


}
