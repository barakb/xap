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

package org.openspaces.persistency.cassandra.meta.mapping;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow.ColumnFamilyRowType;
import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNodeIntrospector;

/**
 * An interface for mapping types to column families and for mapping space documents to column
 * family rows.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public interface SpaceDocumentColumnFamilyMapper {

    /**
     * @param typeDescriptor The type descriptor to convert.
     * @return A {@link ColumnFamilyMetadata} matching the provided typeDescriptor
     */
    ColumnFamilyMetadata toColumnFamilyMetadata(SpaceTypeDescriptor typeDescriptor);

    /**
     * @param columnFamilyRow The column family row to convert.
     * @return A {@link SpaceDocument} matching the column family row data columns.
     */
    SpaceDocument toDocument(ColumnFamilyRow columnFamilyRow);

    /**
     * @param metadata The metadata corresponding the the converted document's type name.
     * @param document The document typ convert.
     * @param rowType  The row type.
     * @return A {@link ColumnFamilyRow} matching the provided docment.
     */
    ColumnFamilyRow toColumnFamilyRow(
            ColumnFamilyMetadata metadata,
            SpaceDocument document,
            ColumnFamilyRowType rowType,
            boolean useDynamicPropertySerializerForDynamicColumns);

    /**
     * @return the introspector used during type introspection/batch opertaions/queries.
     */
    TypeNodeIntrospector getTypeNodeIntrospector();

}
