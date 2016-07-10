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

import org.openspaces.persistency.cassandra.meta.mapping.node.ExternalizableTypeNode;
import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNode;
import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNodeContext;
import org.openspaces.persistency.cassandra.meta.types.PrimitiveClassUtils;
import org.openspaces.persistency.cassandra.meta.types.SerializerProvider;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import me.prettyprint.hector.api.Serializer;

/**
 * A {@link TypeNode} implementations for representing a typed column. I.e. its type is part of the
 * matching static column family metadata.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class TypedColumnMetadata extends AbstractColumnMetadata
        implements ExternalizableTypeNode {
    private static final long serialVersionUID = 1L;
    public static final byte SERIAL_VER = Byte.MIN_VALUE;

    private transient Serializer<?> serializer;

    private String fullName;
    private String name;
    private Class<?> type;

    /* for Externalizable */
    public TypedColumnMetadata() {

    }

    public TypedColumnMetadata(
            String parentFullName,
            String name,
            Class<?> type,
            TypeNodeContext context,
            Serializer<Object> fixedPropertyValueSerializer) {
        serializer = fixedPropertyValueSerializer;
        fullName = (parentFullName != null ? parentFullName + "." : "") + name;
        this.name = name;
        this.type = type;

        initFields();
    }

    private void initFields() {
        if (serializer == null) {
            serializer = SerializerProvider.getSerializer(type);
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
        return type;
    }

    @SuppressWarnings("unchecked")
    public <T> Serializer<T> getSerializer() {
        return (Serializer<T>) serializer;
    }

    public void setSerializer(Serializer<?> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(SERIAL_VER);
        IOUtils.writeString(out, name);
        IOUtils.writeString(out, fullName);
        IOUtils.writeString(out, type.getName());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        in.readByte(); // SERIAL_VER (currently, unused)
        name = IOUtils.readString(in);
        fullName = IOUtils.readString(in);

        String typeName = IOUtils.readString(in);
        if (PrimitiveClassUtils.isPrimitive(typeName)) {
            type = PrimitiveClassUtils.getPrimitive(typeName);
        } else {
            type = Class.forName(typeName, false, Thread.currentThread().getContextClassLoader());
        }

        initFields();
    }

    @Override
    public String toString() {
        return "TypedColumnMetadata [fullName=" + fullName + ", type=" + type + "]";
    }

}