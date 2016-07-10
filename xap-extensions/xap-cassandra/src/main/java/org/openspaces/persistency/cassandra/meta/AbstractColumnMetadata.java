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

package org.openspaces.persistency.cassandra.meta;

import org.openspaces.persistency.cassandra.meta.data.ColumnData;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;
import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNode;
import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNodeContext;

/**
 * Base class for {@link ColumnMetadata} implementations
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
abstract public class AbstractColumnMetadata implements ColumnMetadata, TypeNode {
    @Override
    public void writeToColumnFamilyRow(Object value, ColumnFamilyRow row,
                                       TypeNodeContext context) {
        row.addColumnData(new ColumnData(value, this));
    }

    @Override
    public Object readFromColumnFamilyRow(ColumnFamilyRow row, TypeNodeContext context) {
        ColumnData column = row.getColumn(getFullName());
        if (column == null) {
            return null;
        }

        return column.getValue();
    }
}
