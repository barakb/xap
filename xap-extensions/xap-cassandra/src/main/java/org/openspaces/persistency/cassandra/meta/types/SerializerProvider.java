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

package org.openspaces.persistency.cassandra.meta.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.BigDecimalSerializer;
import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CharSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleTypeSerializer;
import me.prettyprint.cassandra.serializers.FloatTypeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.ShortSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Serializer;

/**
 * A map based {@link Serializer} provider inspired by hector's {@link SerializerTypeInferer}
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class SerializerProvider {

    private static final Map<Class<?>, Serializer<?>> serializers = new HashMap<Class<?>, Serializer<?>>();

    private static final Serializer<Object> OBJECT_SERIALIZER = new ObjectSerializer(SerializerProvider.class.getClassLoader());

    static {
        serializers.put(boolean.class, BooleanSerializer.get());
        serializers.put(Boolean.class, BooleanSerializer.get());
        serializers.put(byte.class, ByteSerializer.get());
        serializers.put(Byte.class, ByteSerializer.get());
        serializers.put(char.class, CharSerializer.get());
        serializers.put(Character.class, CharSerializer.get());
        serializers.put(short.class, ShortSerializer.get());
        serializers.put(Short.class, ShortSerializer.get());
        serializers.put(int.class, IntegerSerializer.get());
        serializers.put(Integer.class, IntegerSerializer.get());
        serializers.put(long.class, LongSerializer.get());
        serializers.put(Long.class, LongSerializer.get());
        serializers.put(float.class, FloatTypeSerializer.get());
        serializers.put(Float.class, FloatTypeSerializer.get());
        serializers.put(double.class, DoubleTypeSerializer.get());
        serializers.put(Double.class, DoubleTypeSerializer.get());
        serializers.put(BigInteger.class, BigIntegerSerializer.get());
        serializers.put(BigDecimal.class, BigDecimalSerializer.get());
        serializers.put(byte[].class, BytesArraySerializer.get());
        serializers.put(ByteBuffer.class, ByteBufferSerializer.get());
        serializers.put(Date.class, DateSerializer.get());
        serializers.put(String.class, StringSerializer.get());
        serializers.put(UUID.class, UUIDSerializer.get());
    }

    /**
     * @param type The type used to infer the matchin {@link Serializer}
     * @return The matching {@link Serializer} and {@link ObjectSerializer} if no matching
     * serializer is found.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> Serializer<T> getSerializer(Class<?> type) {
        Serializer serializer = serializers.get(type);

        if (serializer == null) {
            serializer = getObjectSerializer();
        }

        return serializer;
    }

    /**
     * @return An object serializer that uses the class loader of this class.
     */
    public static Serializer<Object> getObjectSerializer() {
        return OBJECT_SERIALIZER;
    }

    /**
     * @param type The type to test.
     * @return <code>true</code> if this type is serialized using {@link ObjectSerializer}
     */
    public static boolean isCommonJavaType(Class<?> type) {
        return !(getSerializer(type) == getObjectSerializer());
    }
}
