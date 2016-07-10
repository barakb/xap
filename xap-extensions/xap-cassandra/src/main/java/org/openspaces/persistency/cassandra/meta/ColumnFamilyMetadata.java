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

import com.gigaspaces.internal.io.IOUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.persistency.cassandra.meta.conversion.ColumnFamilyNameConverter;
import org.openspaces.persistency.cassandra.meta.mapping.node.TopLevelTypeNode;
import org.openspaces.persistency.cassandra.meta.types.SerializerProvider;
import org.openspaces.persistency.support.SpaceTypeDescriptorContainer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.Serializer;

/**
 * @author Dan Kilman
 * @since 9.1.1
 */
public class ColumnFamilyMetadata implements Externalizable {

    private static final Log logger = LogFactory.getLog(ColumnFamilyMetadata.class);

    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VER = Byte.MIN_VALUE;

    private final transient Map<String, TypedColumnMetadata> columns = new HashMap<String, TypedColumnMetadata>();
    private final transient Set<String> indexes = new HashSet<String>();

    private transient Serializer<?> keySerializer;
    private transient String typeName;
    private transient String keyName;
    private transient Class<?> keyType;

    private SpaceTypeDescriptorContainer typeDescriptorData;
    private TopLevelTypeNode topLevelTypeNode;
    private String columnFamilyName;

    /* for Externalizable */
    public ColumnFamilyMetadata() {

    }

    public ColumnFamilyMetadata(
            TopLevelTypeNode topLevelTypeNode,
            Set<String> initialIndexes,
            ColumnFamilyNameConverter columnFamilyNameConverter,
            SpaceTypeDescriptorContainer typeDescriptorData) {
        this.topLevelTypeNode = topLevelTypeNode;
        this.typeDescriptorData = typeDescriptorData;
        if (initialIndexes != null) {
            indexes.addAll(initialIndexes);
        }

        initFieldsFromTopLevelTypeNode();

        columnFamilyName = columnFamilyNameConverter.toColumnFamilyName(typeName);

    }

    private void initFieldsFromTopLevelTypeNode() {
        typeName = topLevelTypeNode.getTypeName();
        keyName = topLevelTypeNode.getKeyName();
        keyType = topLevelTypeNode.getKeyType();
        keySerializer = SerializerProvider.getSerializer(keyType);
        columns.putAll(topLevelTypeNode.getAllTypedColumnMetadataChildren());
    }

    public SpaceTypeDescriptorContainer getTypeDescriptorData() {
        return typeDescriptorData;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public String getKeyName() {
        return keyName;
    }

    public Class<?> getKeyType() {
        return keyType;
    }

    public Serializer<?> getKeySerializer() {
        return keySerializer;
    }

    public Map<String, TypedColumnMetadata> getColumns() {
        return columns;
    }

    public TopLevelTypeNode getTopLevelTypeNode() {
        return topLevelTypeNode;
    }

    /* 
     * only available at the synchronization endpoint,
     * will be empty at the space data source as it is never used 
     * by it
     */
    public Set<String> getIndexes() {
        return indexes;
    }

    public void setFixedPropertySerializerForTypedColumn(Serializer<?> serializer) {
        for (TypedColumnMetadata typeColumnMetadata : columns.values()) {
            // primitive types for fixed properties columns are always serialized
            // using native serialization
            if (!SerializerProvider.isCommonJavaType(typeColumnMetadata.getType())) {
                typeColumnMetadata.setSerializer(serializer);
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(SERIAL_VER);
        IOUtils.writeObject(out, typeDescriptorData);
        IOUtils.writeObject(out, topLevelTypeNode);
        IOUtils.writeString(out, columnFamilyName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        in.readByte(); // serial_ver (currently, unused)
        typeDescriptorData = IOUtils.readObject(in);
        topLevelTypeNode = IOUtils.readObject(in);
        columnFamilyName = IOUtils.readString(in);
        initFieldsFromTopLevelTypeNode();
    }

    @Override
    public String toString() {
        return "ColumnFamilyMetadata [" +
                "  typeName=" + typeName +
                ", columnFamilyName=" + columnFamilyName +
                ", keyName=" + keyName +
                ", keyType=" + keyType +
                ", indexes=" + indexes +
                ", " + (logger.isTraceEnabled() ? "columns=" + columns.values() : "columnsSize=" + columns.size()) +
                " ]";
    }

}
