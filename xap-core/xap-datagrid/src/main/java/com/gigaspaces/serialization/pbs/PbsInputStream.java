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

package com.gigaspaces.serialization.pbs;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.serialization.BinaryObject;
import com.j_spaces.core.exception.internal.PBSInternalSpaceException;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * This class is an extension of a ByteArrayInputStream with PBS functionality.
 *
 * @author niv
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class PbsInputStream extends GSByteArrayInputStream implements IThreadLocalResource {
    private boolean _isUsed;

    public PbsInputStream(byte buf[]) {
        super(buf);
    }

    public short readShort() {
        return (short) readInt();
    }

    public int readInt() {
        int b = readUnsignedByte();
        int value = b & 0x3f;
        int cBits = 6;
        boolean fNeg = (b & 0x40) != 0;
        while ((b & 0x80) != 0) {
            b = readUnsignedByte();
            value |= (b & 0x7f) << cBits;
            cBits += 7;
        }
        if (fNeg)
            value = ~value;
        return value;
    }

    public int readIntFixed() {
        int b1 = readUnsignedByte();
        int b2 = readUnsignedByte();
        int b3 = readUnsignedByte();
        int b4 = readUnsignedByte();
        return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
    }

    public long readLong() {
        int b = readUnsignedByte();
        long l = b & 0x3f;
        int cBits = 6;
        boolean fNeg = (b & 0x40) != 0;
        while ((b & 0x80) != 0) {
            b = readUnsignedByte();
            l |= (long) (b & 0x7f) << cBits;
            cBits += 7;
        }
        if (fNeg)
            l = ~l;
        return l;
    }

    public long readLongFixed() {
        long value = (long) readUnsignedByte() << 56;
        value += (long) readUnsignedByte() << 48;
        value += (long) readUnsignedByte() << 40;
        value += (long) readUnsignedByte() << 32;
        value += (long) readUnsignedByte() << 24;
        value += (long) readUnsignedByte() << 16;
        value += (long) readUnsignedByte() << 8;
        value += (long) readUnsignedByte() << 0;
        return value;
    }

    public float readFloat() {
        int rawValue = readInt();
        return Float.intBitsToFloat(rawValue);
    }

    public double readDouble() {
        long rawValue = readLong();
        return Double.longBitsToDouble(rawValue);
    }

    public boolean readBoolean() {
        return _buffer[_position++] == 1;
    }

    public char readChar() {
        return (char) readInt();
    }

    public String readString() {
        // Read string type
        byte stringType = readByte();
        switch (stringType) {
            case PbsStringType.NULL:
                return null;
            case PbsStringType.EMPTY:
                return "";
            case PbsStringType.ASCII: {
                // Read length:
                int length = readInt();
                // Create characters buffer:
                char[] chars = new char[length];
                // Read characters:
                for (int i = 0; i < length; chars[i++] = (char) readUnsignedByte()) ;

                return new String(chars);
            }
            case PbsStringType.UNICODE: {
                // Read length:
                int length = readInt();
                // Create characters buffer:
                char[] chars = new char[length];
                // Read characters:
                for (int i = 0; i < length; i++) {
                    int secondByte = _buffer[_position + length] & 0xff;
                    int firstByte = readUnsignedByte();
                    chars[i] = (char) ((secondByte << 8) | firstByte);
                }
                _position += length;
                return new String(chars);
            }
            default:
                throw new PBSInternalSpaceException("PBS failed - unsupported string type: " + stringType);
        }
    }

    public String readRepetitiveString() {
        long stringKey = readLong();
        return PbsRepetitiveStringCache.getCache().get(stringKey);
    }

    public String[] readRepetitiveStringArray() {
        int size = readInt();
        if (size == -1)
            return null;

        String[] stringArray = new String[size];

        for (int i = 0; i < size; i++)
            stringArray[i] = readRepetitiveString();
        return stringArray;
    }

    public Date readDateTime() {
        long rawValue = readLong();
        return new Date(rawValue);
    }

    public BigDecimal readDecimal() {
        String rawValue = readString();
        return new BigDecimal(rawValue);
    }

    public UUID readUUID() {
        long least = readLongFixed();
        long most = readLongFixed();
        return new UUID(most, least);
    }

    public byte[] readByteArray() {
        int length = readInt();
        if (length == -1) {
            return null;
        } else {
            byte[] result = new byte[length];
            read(result, 0, result.length);
            return result;
        }
    }

    public byte[] readFixedByteArray() {
        int length = readIntFixed();
        if (length == -1) {
            return null;
        } else {
            byte[] result = new byte[length];
            read(result, 0, result.length);
            return result;
        }
    }

    public short[] readShortArray() {
        int length = readInt();
        short[] array = new short[length];
        for (int i = 0; i < length; i++)
            array[i] = readShort();
        return array;
    }

    public int[] readIntArray() {
        int length = readInt();
        if (length < 0)
            return null;
        int[] array = new int[length];
        for (int i = 0; i < length; i++)
            array[i] = readInt();
        return array;
    }

    public long[] readLongArray() {
        int length = readInt();
        long[] array = new long[length];
        for (int i = 0; i < length; i++)
            array[i] = readLong();
        return array;
    }

    public float[] readFloatArray() {
        int length = readInt();
        float[] array = new float[length];
        for (int i = 0; i < length; i++)
            array[i] = readFloat();
        return array;
    }

    public double[] readDoubleArray() {
        int length = readInt();
        double[] array = new double[length];
        for (int i = 0; i < length; i++)
            array[i] = readDouble();
        return array;
    }

    public boolean[] readBoolArray() {
        int length = readInt();
        boolean[] array = new boolean[length];
        for (int i = 0; i < length; i++)
            array[i] = readBoolean();
        return array;
    }

    public char[] readCharArray() {
        int length = readInt();
        char[] array = new char[length];
        for (int i = 0; i < length; i++)
            array[i] = readChar();
        return array;
    }

    public String[] readStringArray() {
        int length = readInt();

        if (length == -1)
            return null;

        String[] array = new String[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readString();
        }
        return array;
    }

    public Date[] readDateTimeArray() {
        int length = readInt();
        Date[] array = new Date[length];
        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readDateTime();
        }
        return array;
    }

    public BigDecimal[] readDecimalArray() {
        int length = readInt();
        BigDecimal[] array = new BigDecimal[length];
        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readDecimal();
        }
        return array;
    }

    public UUID[] readUUIDArray() {
        int length = readInt();
        UUID[] array = new UUID[length];
        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readUUID();
        }
        return array;
    }

    public Byte[] readByteWrapperArray() {
        int length = readInt();
        Byte[] array = new Byte[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = _buffer[_position++];
        }
        return array;
    }

    public Short[] readShortWrapperArray() {
        int length = readInt();
        Short[] array = new Short[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readShort();
        }
        return array;
    }

    public Integer[] readIntegerWrapperArray() {
        int length = readInt();
        Integer[] array = new Integer[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readInt();
        }
        return array;
    }

    public Long[] readLongWrapperArray() {
        int length = readInt();
        Long[] array = new Long[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readLong();
        }
        return array;
    }

    public Float[] readFloatWrapperArray() {
        int length = readInt();
        Float[] array = new Float[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readFloat();
        }
        return array;
    }

    public Double[] readDoubleWrapperArray() {
        int length = readInt();
        Double[] array = new Double[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readDouble();
        }
        return array;
    }

    public Boolean[] readBooleanWrapperArray() {
        int length = readInt();
        Boolean[] array = new Boolean[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readBoolean();
        }
        return array;
    }

    public Character[] readCharWrapperArray() {
        int length = readInt();
        Character[] array = new Character[length];

        for (int i = 0; i < length; i++) {
            byte isNull = _buffer[_position++];
            if (isNull != PbsTypeInfo.NULL)
                array[i] = readChar();
        }
        return array;
    }

    public Properties readProperties() {
        int length = readInt();
        Properties result = new Properties();
        for (int i = 0; i < length; ++i) {
            String key = readString();
            String value = readString();
            result.put(key, value);
        }
        return result;
    }

    public Map<String, String> readStringProperties() {
        int length = readInt();
        if (length == -1)
            return null;
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (int i = 0; i < length; ++i) {
            String key = readString();
            String value = readString();
            result.put(key, value);
        }
        return result;
    }

    public BinaryObject readBinaryObject() {
        // Read format code:
        byte formatCode = _buffer[_position++];
        // Read data length:
        int length = readIntFixed();
        byte[] data = new byte[length];
        // Read buffer (NOTE: this specific overload gives best performance).
        read(data, 0, length);
        // return result:
        return new BinaryObject(formatCode, data);
    }

    public Boolean readNullableBoolean() {
        byte value = _buffer[_position++];
        switch (value) {
            case 0:
                return Boolean.FALSE;
            case 1:
                return Boolean.TRUE;
            default:
                return null;
        }
    }

    public Integer readNullableInt() {
        if (readBoolean())
            return readInt();
        return null;
    }

    public Long readNullableLong() {
        if (readBoolean())
            return readLong();
        return null;
    }

    public UUID readNullableUUID() {
        if (readBoolean())
            return readUUID();
        return null;
    }

    public boolean isUsed() {
        return _isUsed;
    }

    public void release() {
        _isUsed = false;
    }

    public void setAsUsed() {
        _isUsed = true;
    }

}
