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

import org.openspaces.persistency.cassandra.meta.DynamicColumnMetadata;
import org.openspaces.persistency.cassandra.meta.TypedColumnMetadata;
import org.openspaces.persistency.cassandra.meta.data.ColumnData;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;

import java.util.HashMap;
import java.util.Map;


/**
 * Base class for {@link CompoundTypeNode} implementations.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
abstract public class AbstractCompoundTypeNode implements CompoundTypeNode {

    private final Map<String, TypeNode> children = new HashMap<String, TypeNode>();

    @Override
    public Map<String, TypedColumnMetadata> getAllTypedColumnMetadataChildren() {
        Map<String, TypedColumnMetadata> result = new HashMap<String, TypedColumnMetadata>();
        for (TypeNode child : children.values()) {
            if (child instanceof TypedColumnMetadata) {
                TypedColumnMetadata columnMetadata = (TypedColumnMetadata) child;
                result.put(child.getFullName(), columnMetadata);
            } else if (child instanceof DynamicColumnMetadata) {
                throw new IllegalStateException("Dynamic columns should not be part of the column " +
                        "family metadata static columns");
            } else {
                result.putAll(((CompoundTypeNode) child).getAllTypedColumnMetadataChildren());
            }
        }
        return result;
    }

    @Override
    public void writeToColumnFamilyRow(Object value, ColumnFamilyRow row,
                                       TypeNodeContext context) {
        context.increaseNestingLevel();
        try {
            if (context.isDynamic()) {
                String dynamicHeaderColumnName = getDynamicHeaderColumnName();
                if (dynamicHeaderColumnName != null) {
                    row.addColumnData(new ColumnData(dynamicHeaderColumnName,
                            new DynamicColumnMetadata(getFullName(),
                                    context.getTypeNodeIntrospector()
                                            .getDynamicPropertyValueSerializer())));
                }
            }

            writeToColumnFamilyRowImpl(value, row, context);
        } finally {
            context.descreaseNestingLevel();
        }
    }

    protected void writePropertyToColumnFamilyRow(
            ColumnFamilyRow row,
            String propertyName,
            Object propertyValue,
            TypeNodeContext context) {
        final boolean parentIsDynamicContext = context.isDynamic();
        boolean currentIsDynamicContext = parentIsDynamicContext;

        TypeNode typeNode = getChildren().get(propertyName);
        if (typeNode == null) {
            currentIsDynamicContext = true;
            context.setDynamic(currentIsDynamicContext);
            try {
                typeNode = context.getTypeNodeIntrospector().introspect(getFullName(),
                        propertyName,
                        propertyValue,
                        context);
            } finally {
                context.setDynamic(parentIsDynamicContext);
            }
        }

        context.setDynamic(currentIsDynamicContext);
        try {
            typeNode.writeToColumnFamilyRow(propertyValue, row, context);
        } finally {
            context.setDynamic(parentIsDynamicContext);
        }
    }

    abstract protected void writeToColumnFamilyRowImpl(Object value, ColumnFamilyRow row,
                                                       TypeNodeContext context);

    /**
     * controls how this type will be read from cassandra, see implementations and
     * SpaceDocumentTopLevelTypeNode. returning null will result by child property values being put
     * in a HashMap
     */
    abstract protected String getDynamicHeaderColumnName();

    protected Map<String, TypeNode> getChildren() {
        return children;
    }

}
