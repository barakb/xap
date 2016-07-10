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

package org.openspaces.persistency.cassandra.meta.mapping.node;

import org.openspaces.persistency.cassandra.CassandraPersistencyConstants;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;

import java.util.Map;


/**
 * {@link java.util.Map} based {@link CompoundTypeNode} (currently, unused)
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class MapTypeNode extends AbstractCompoundTypeNode {

    private final String fullName;

    public MapTypeNode(String fullName) {
        this.fullName = fullName;
    }

    @Override
    public String getFullName() {
        return fullName;
    }

    @Override
    protected String getDynamicHeaderColumnName() {
        return CassandraPersistencyConstants.MAP_ENTRY_COLUMN;
    }

    // maps are not necessarily string -> object
    // for this reason, this TypeNode is currently not being used
    @SuppressWarnings("unchecked")
    @Override
    protected void writeToColumnFamilyRowImpl(Object value,
                                              ColumnFamilyRow row, TypeNodeContext context) {
        Map<String, Object> map = (Map<String, Object>) value;
        for (Map.Entry<String, Object> property : map.entrySet()) {
            String propertyName = property.getKey();
            Object propertyValue = property.getValue();

            if (propertyValue == null) {
                continue;
            }

            writePropertyToColumnFamilyRow(row, propertyName, propertyValue, context);
        }
    }

    @Override
    public Object readFromColumnFamilyRow(ColumnFamilyRow row, TypeNodeContext context) {
        throw new UnsupportedOperationException("Not implemented in dynamic columns");
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException("Not implemented in dynamic columns");
    }

    @Override
    public Class<?> getType() {
        throw new UnsupportedOperationException("Not implemented in dynamic columns");
    }
}
