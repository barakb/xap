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

package org.openspaces.persistency.cassandra.meta.types.dynamic;

import org.openspaces.persistency.cassandra.error.SpaceCassandraSerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.IntegerSerializer;


/**
 * Serialized objects will contain a prefix denoting the type of the value and will be deserialized
 * appropriately based on this prefix. Default serialization machanizm for non primitive, non common
 * java types is standard object serialization.
 *
 * {@link Object}/{@link java.math.BigInteger}/{@link java.math.BigDecimal} serialization logic
 * borrowed from hector serializers, rest is a modification of {@link
 * "https://gist.github.com/850865"}
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class DynamicPropertyValueSerializer implements PropertyValueSerializer {

    private static final DynamicPropertyValueSerializer INSTANCE = new DynamicPropertyValueSerializer();

    public static DynamicPropertyValueSerializer get() {
        return INSTANCE;
    }

    private DynamicPropertyValueSerializer() {

    }

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final byte TYPE_CHAR = Byte.MIN_VALUE;
    private static final byte TYPE_BYTE = Byte.MIN_VALUE + 1;
    private static final byte TYPE_STRING = Byte.MIN_VALUE + 2;
    private static final byte TYPE_BOOLEAN = Byte.MIN_VALUE + 3;
    private static final byte TYPE_INT = Byte.MIN_VALUE + 4;
    private static final byte TYPE_LONG = Byte.MIN_VALUE + 5;
    private static final byte TYPE_FLOAT = Byte.MIN_VALUE + 6;
    private static final byte TYPE_DOUBLE = Byte.MIN_VALUE + 7;
    private static final byte TYPE_SHORT = Byte.MIN_VALUE + 8;
    private static final byte TYPE_UUID = Byte.MIN_VALUE + 9;
    private static final byte TYPE_DATE = Byte.MIN_VALUE + 10;
    private static final byte TYPE_BIGINT = Byte.MIN_VALUE + 11;
    private static final byte TYPE_BIGDECIMAL = Byte.MIN_VALUE + 12;
    private static final byte TYPE_BYTEARRAY = Byte.MIN_VALUE + 13;

    private static final byte TYPE_OBJECT = Byte.MAX_VALUE;

    private static final Map<Class<?>, Byte> typeCodes = new HashMap<Class<?>, Byte>();

    static {
        typeCodes.put(Boolean.class, TYPE_BOOLEAN);
        typeCodes.put(Byte.class, TYPE_BYTE);
        typeCodes.put(Character.class, TYPE_CHAR);
        typeCodes.put(Short.class, TYPE_SHORT);
        typeCodes.put(Integer.class, TYPE_INT);
        typeCodes.put(Long.class, TYPE_LONG);
        typeCodes.put(Float.class, TYPE_FLOAT);
        typeCodes.put(Double.class, TYPE_DOUBLE);
        typeCodes.put(String.class, TYPE_STRING);
        typeCodes.put(UUID.class, TYPE_UUID);
        typeCodes.put(Date.class, TYPE_DATE);
        typeCodes.put(BigInteger.class, TYPE_BIGINT);
        typeCodes.put(BigDecimal.class, TYPE_BIGDECIMAL);
        typeCodes.put(byte[].class, TYPE_BYTEARRAY);
    }

    private byte type(Object value) {
        Byte type = typeCodes.get(value.getClass());
        if (type == null) {
            type = TYPE_OBJECT;
        }
        return type;
    }

    @Override
    public ByteBuffer toByteBuffer(Object obj) {
        switch (type(obj)) {
            case TYPE_CHAR:
                return charToByteBuffer((Character) obj);
            case TYPE_BYTE:
                return byteToByteBuffer((Byte) obj);
            case TYPE_STRING:
                return stringToByteBuffer((String) obj);
            case TYPE_BOOLEAN:
                return booleanToByteBuffer((Boolean) obj);
            case TYPE_INT:
                return intToByteBuffer((Integer) obj);
            case TYPE_LONG:
                return longToByteBuffer((Long) obj);
            case TYPE_FLOAT:
                return floatToByteBuffer((Float) obj);
            case TYPE_DOUBLE:
                return doubleToByteBuffer((Double) obj);
            case TYPE_SHORT:
                return shortToByteBuffer((Short) obj);
            case TYPE_UUID:
                return UUIDToByteBuffer((UUID) obj);
            case TYPE_DATE:
                return dateToByteBuffer((Date) obj);
            case TYPE_BIGINT:
                return bigIntToByteBuffer((BigInteger) obj);
            case TYPE_BIGDECIMAL:
                return bigDecimalToByteBuffer((BigDecimal) obj);
            case TYPE_BYTEARRAY:
                return byteArrayToByteBuffer((byte[]) obj);
        }

        return objectToByteBuffer(obj);
    }

    @Override
    public Object fromByteBuffer(ByteBuffer byteBuffer) {
        if ((byteBuffer == null) || !byteBuffer.hasRemaining())
            return null;
        switch (byteBuffer.get()) {
            case TYPE_CHAR:
                return UTF_8.decode(byteBuffer).toString().charAt(0);
            case TYPE_BYTE:
                return byteBuffer.get();
            case TYPE_STRING:
                return UTF_8.decode(byteBuffer).toString();
            case TYPE_BOOLEAN:
                return byteBuffer.get() == (byte) 1;
            case TYPE_INT:
                return byteBuffer.getInt();
            case TYPE_LONG:
                return byteBuffer.getLong();
            case TYPE_FLOAT:
                return byteBuffer.getFloat();
            case TYPE_DOUBLE:
                return byteBuffer.getDouble();
            case TYPE_SHORT:
                return byteBuffer.getShort();
            case TYPE_UUID:
                return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
            case TYPE_DATE:
                return new Date(byteBuffer.getLong());
            case TYPE_BIGINT:
                return byteBufferToBigInteger(byteBuffer);
            case TYPE_BIGDECIMAL:
                return byteBufferToBigDecimal(byteBuffer);
            case TYPE_BYTEARRAY:
                return byteBufferToByteArray(byteBuffer);
        }

        return byteBuffertoObject(byteBuffer);
    }

    private ByteBuffer booleanToByteBuffer(Boolean value) {
        byte[] bytes = new byte[1 + 1];
        bytes[0] = TYPE_BOOLEAN;
        bytes[1] = value ? (byte) 1 : (byte) 0;
        return ByteBuffer.wrap(bytes);
    }

    private ByteBuffer byteToByteBuffer(Byte value) {
        byte[] bytes = new byte[1 + 1];
        bytes[0] = TYPE_BYTE;
        bytes[1] = value;
        return ByteBuffer.wrap(bytes);
    }

    private ByteBuffer charToByteBuffer(Character value) {
        return stringToByteBuffer(new String(new char[]{value}), TYPE_CHAR);
    }

    private ByteBuffer stringToByteBuffer(String value) {
        return stringToByteBuffer(value, TYPE_STRING);
    }

    private ByteBuffer stringToByteBuffer(String value, byte type) {
        try {
            byte[] stringUTFBytes = value.getBytes(UTF_8.name());
            byte[] bytes = new byte[1 + stringUTFBytes.length];
            bytes[0] = type;
            System.arraycopy(stringUTFBytes, 0, bytes, 1, stringUTFBytes.length);
            return ByteBuffer.wrap(bytes);
        } catch (UnsupportedEncodingException e) {
            throw new SpaceCassandraSerializationException("Failed serializing string: " + value, e);
        }
    }

    private ByteBuffer shortToByteBuffer(Short value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 2);
        byteBuffer.put(0, TYPE_SHORT);
        byteBuffer.putShort(1, value);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private ByteBuffer intToByteBuffer(Integer value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 4);
        byteBuffer.put(0, TYPE_INT);
        byteBuffer.putInt(1, value);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private ByteBuffer dateToByteBuffer(Date value) {
        return longToByteBuffer(value.getTime(), TYPE_DATE);
    }

    private ByteBuffer longToByteBuffer(Long value) {
        return longToByteBuffer(value, TYPE_LONG);
    }

    private ByteBuffer longToByteBuffer(Long value, byte type) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 8);
        byteBuffer.put(0, type);
        byteBuffer.putLong(1, value);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private ByteBuffer floatToByteBuffer(Float value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 4);
        byteBuffer.put(0, TYPE_FLOAT);
        byteBuffer.putFloat(1, value);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private ByteBuffer doubleToByteBuffer(Double value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 8);
        byteBuffer.put(0, TYPE_DOUBLE);
        byteBuffer.putDouble(1, value);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private ByteBuffer UUIDToByteBuffer(UUID value) {
        long msb = value.getMostSignificantBits();
        long lsb = value.getLeastSignificantBits();
        byte[] bytes = new byte[1 + 8 + 8];

        bytes[0] = TYPE_UUID;
        for (int i = 0; i < 8; i++)
            bytes[i + 1] = (byte) (msb >>> 8 * (7 - i));
        for (int i = 8; i < 16; i++)
            bytes[i + 1] = (byte) (lsb >>> 8 * (7 - i));

        return ByteBuffer.wrap(bytes);
    }

    private BigInteger byteBufferToBigInteger(ByteBuffer byteBuffer) {
        int length = byteBuffer.remaining();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        return new BigInteger(bytes);
    }

    private ByteBuffer bigIntToByteBuffer(BigInteger value) {
        byte[] bigIntBytes = value.toByteArray();
        byte[] bytes = new byte[1 + bigIntBytes.length];
        bytes[0] = TYPE_BIGINT;
        System.arraycopy(bigIntBytes, 0, bytes, 1, bigIntBytes.length);
        return ByteBuffer.wrap(bytes);
    }

    private BigDecimal byteBufferToBigDecimal(ByteBuffer byteBuffer) {
        int scale = byteBuffer.getInt();
        byte[] bigIntegerBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bigIntegerBytes, 0, byteBuffer.remaining());
        BigInteger bigInteger = new BigInteger(bigIntegerBytes);
        return new BigDecimal(bigInteger, scale);
    }

    private ByteBuffer bigDecimalToByteBuffer(BigDecimal value) {
        byte[] bigIntBytes = value.unscaledValue().toByteArray();
        byte[] scaleBytes = IntegerSerializer.get().toBytes(value.scale());
        byte[] bytes = new byte[1 + scaleBytes.length + bigIntBytes.length];
        bytes[0] = TYPE_BIGDECIMAL;
        System.arraycopy(scaleBytes, 0, bytes, 1, scaleBytes.length);
        System.arraycopy(bigIntBytes, 0, bytes, 1 + scaleBytes.length, bigIntBytes.length);
        return ByteBuffer.wrap(bytes);
    }

    private Object byteBufferToByteArray(ByteBuffer byteBuffer) {
        int length = byteBuffer.getInt();
        byte[] result = new byte[length];
        byteBuffer.get(result);
        return result;
    }

    private ByteBuffer byteArrayToByteBuffer(byte[] value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + 4 + value.length);
        byteBuffer.put(TYPE_BYTEARRAY);
        byteBuffer.putInt(value.length);
        byteBuffer.put(value);
        byteBuffer.rewind();
        return byteBuffer;
    }

    private Object byteBuffertoObject(ByteBuffer byteBuffer) {
        try {
            int length = byteBuffer.remaining();
            ByteArrayInputStream bais = new ByteArrayInputStream(byteBuffer.array(),
                    byteBuffer.arrayOffset() + byteBuffer.position(),
                    length);
            ObjectInputStream ois;
            ois = new ObjectInputStream(bais);
            Object object = ois.readObject();
            byteBuffer.position(byteBuffer.position() + (length - ois.available()));
            ois.close();
            return object;
        } catch (Exception e) {
            throw new SpaceCassandraSerializationException("Failed deserializing object", e);
        }
    }

    private ByteBuffer objectToByteBuffer(Object value) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(value);
            oos.close();
            byte[] objectBytes = baos.toByteArray();
            byte[] bytes = new byte[1 + objectBytes.length];
            bytes[0] = TYPE_OBJECT;
            System.arraycopy(objectBytes, 0, bytes, 1, objectBytes.length);
            return ByteBuffer.wrap(bytes);
        } catch (IOException e) {
            throw new SpaceCassandraSerializationException("Failed serializing object " + value, e);
        }
    }

}
