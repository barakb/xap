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
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.conversion.ColumnFamilyNameConverter;
import org.openspaces.persistency.cassandra.meta.conversion.DefaultColumnFamilyNameConverter;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow.ColumnFamilyRowType;
import org.openspaces.persistency.cassandra.meta.mapping.filter.FlattenedPropertiesFilter;
import org.openspaces.persistency.cassandra.meta.mapping.node.SpaceDocumentTopLevelTypeNode;
import org.openspaces.persistency.cassandra.meta.mapping.node.TopLevelTypeNode;
import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNodeContext;
import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNodeIntrospector;
import org.openspaces.persistency.cassandra.meta.types.dynamic.PropertyValueSerializer;
import org.openspaces.persistency.support.SpaceTypeDescriptorContainer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The default {@link SpaceDocumentColumnFamilyMapper} implementation.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class DefaultSpaceDocumentColumnFamilyMapper
        implements SpaceDocumentColumnFamilyMapper {

    private final TypeNodeIntrospector typeNodeIntrospector;
    private final ColumnFamilyNameConverter columnFamilyNameConverter;

    // used by CassandraSpaceSynchronizationEndpoint
    public DefaultSpaceDocumentColumnFamilyMapper(
            PropertyValueSerializer fixedPropertyValueSerializer,
            PropertyValueSerializer dynamicPropertyValueSerializer,
            FlattenedPropertiesFilter flattenePropertiesFilter,
            ColumnFamilyNameConverter columnFamilyNameConverter) {
        typeNodeIntrospector = new TypeNodeIntrospector(fixedPropertyValueSerializer,
                dynamicPropertyValueSerializer,
                flattenePropertiesFilter);

        if (columnFamilyNameConverter == null) {
            this.columnFamilyNameConverter = new DefaultColumnFamilyNameConverter();
        } else {
            this.columnFamilyNameConverter = columnFamilyNameConverter;
        }
    }

    // used by CassandraSpaceDataSource in which default implementations and nesting will never be used.
    public DefaultSpaceDocumentColumnFamilyMapper(
            PropertyValueSerializer fixedPropertyValueSerializer,
            PropertyValueSerializer dynamicPropertyValueSerializer) {
        this(fixedPropertyValueSerializer,
                dynamicPropertyValueSerializer,
                null,
                null);
    }

    @Override
    public ColumnFamilyMetadata toColumnFamilyMetadata(
            SpaceTypeDescriptor typeDescriptor) {
        SpaceTypeDescriptorContainer typeDescriptorData = new SpaceTypeDescriptorHolder(typeDescriptor);

        Map<String, SpaceIndex> indexes = typeDescriptor.getIndexes();
        Set<String> initialIndexes = new HashSet<String>();
        if (indexes != null) {
            for (SpaceIndex index : indexes.values()) {
                if (index.getIndexType() != SpaceIndexType.NONE &&
                        !index.getName().equals(typeDescriptor.getIdPropertyName())) {
                    initialIndexes.add(index.getName());
                }
            }
        }

        TopLevelTypeNode topLevelTypeNode = typeNodeIntrospector.introspectTypeDescriptor(typeDescriptor);
        return new ColumnFamilyMetadata(topLevelTypeNode,
                initialIndexes,
                columnFamilyNameConverter,
                typeDescriptorData);
    }

    @Override
    public SpaceDocument toDocument(ColumnFamilyRow columnFamilyRow) {
        // currently this is the only supported top level type node
        SpaceDocumentTopLevelTypeNode topLevelTypeNode = (SpaceDocumentTopLevelTypeNode) columnFamilyRow
                .getColumnFamilyMetadata()
                .getTopLevelTypeNode();

        return topLevelTypeNode.readFromColumnFamilyRow(columnFamilyRow,
                new TypeNodeContext(typeNodeIntrospector,
                        true /* not relevant for read context */));
    }

    @Override
    public ColumnFamilyRow toColumnFamilyRow(
            ColumnFamilyMetadata metadata,
            SpaceDocument document,
            ColumnFamilyRowType type,
            boolean useDynamicPropertySerializerForDynamicColumns) {
        String keyName = metadata.getKeyName();
        Object keyValue = document.getProperty(keyName);
        ColumnFamilyRow row = new ColumnFamilyRow(metadata, keyValue, type);
        metadata.getTopLevelTypeNode()
                .writeToColumnFamilyRow(document, row,
                        new TypeNodeContext(typeNodeIntrospector,
                                useDynamicPropertySerializerForDynamicColumns));
        return row;
    }

    @Override
    public TypeNodeIntrospector getTypeNodeIntrospector() {
        return typeNodeIntrospector;
    }
}
