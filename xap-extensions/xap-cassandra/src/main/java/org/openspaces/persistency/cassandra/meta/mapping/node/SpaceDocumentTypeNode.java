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

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.entry.VirtualEntry;

import org.openspaces.persistency.cassandra.CassandraPersistencyConstants;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;

import java.util.Map;

/**
 * A {@link VirtualEntry} based implementation of {@link CompoundTypeNode}.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class SpaceDocumentTypeNode extends AbstractCompoundTypeNode {
    // _name if this is a direct child of the top level type node
    // path from top level type node otherwise
    protected String fullName;

    protected String name;

    // virtual entrie's type name
    protected String typeName;

    public SpaceDocumentTypeNode() {

    }

    // if this is a top level document than name is the keyname, otherwise it is the property simple name
    public SpaceDocumentTypeNode(String typeName,
                                 String parentFullName,
                                 String name,
                                 Map<String, TypeNode> initialChildren,
                                 TypeNodeContext context) {
        this.typeName = typeName;
        this.fullName = generateFullName(parentFullName, name);
        this.name = name;
        if (initialChildren != null) {
            getChildren().putAll(initialChildren);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getFullName() {
        return fullName;
    }

    @Override
    public Class<?> getType() {
        return SpaceDocument.class;
    }

    public String getTypeName() {
        return typeName;
    }

    protected String generateFullName(String parentFullName, String name) {
        return (parentFullName != null ? parentFullName + "." : "") + name;
    }

    protected boolean shouldSkipEntryWrite(String key, Object value) {
        return value == null;
    }

    @Override
    protected String getDynamicHeaderColumnName() {
        return CassandraPersistencyConstants.SPACE_DOCUMENT_COLUMN_PREFIX + typeName;
    }

    @Override
    protected void writeToColumnFamilyRowImpl(Object value, ColumnFamilyRow row, TypeNodeContext context) {
        VirtualEntry entry = (VirtualEntry) value;
        for (Map.Entry<String, Object> property : entry.getProperties().entrySet()) {
            String propertyName = property.getKey();
            Object propertyValue = property.getValue();

            if (shouldSkipEntryWrite(propertyName, propertyValue)) {
                continue;
            }

            writePropertyToColumnFamilyRow(row, propertyName, propertyValue, context);
        }
    }

    // only used by top level type node
    // as dynamic properties are read in a different part
    @Override
    public SpaceDocument readFromColumnFamilyRow(ColumnFamilyRow row, TypeNodeContext context) {
        SpaceDocument result = new SpaceDocument(getTypeName());
        for (Map.Entry<String, TypeNode> entry : getChildren().entrySet()) {
            String name = entry.getKey();
            TypeNode child = entry.getValue();
            Object value = child.readFromColumnFamilyRow(row, context);
            if (value != null) {
                result.setProperty(name, value);
            }
        }
        return result;
    }

}
