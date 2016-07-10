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

import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.serialization.BinaryObject;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * This class is an extension of a ByteArrayOutputStream with PBS functionality.
 *
 * @author niv
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class PbsOutputStream extends GSByteArrayOutputStream implements IThreadLocalResource {
    protected static final int SIZE_INT = 4;
    protected static final int SIZE_LONG = 8;
    protected static final int SIZE_UUID = 16;
    private boolean _isUsed;

    public PbsOutputStream() {
        super();
    }

    public PbsOutputStream(int size) {
        super(size);
    }

    public void writeShort(short value) {
        writeInt(value);
    }

    public void writeInt(int value) {
        byte b = 0;
        if (value < 0) {
            b = 64;
            value = ~value;
        }
        b |= (byte) (value & 0x3f);

        for (value = (int) (value >> 6); value != 0; value = (int) (value >> 7)) {
            b |= 0x80;
            ensureCapacity(1);
            _buffer[_count++] = b;
            b = (byte) (value & 0x7f);
        }
        ensureCapacity(1);
        _buffer[_count++] = b;
    }

    public void writeIntFixed(int value) {
        ensureCapacity(SIZE_INT);
        _buffer[_count++] = (byte) ((value >>> 24) & 0xFF);
        _buffer[_count++] = (byte) ((value >>> 16) & 0xFF);
        _buffer[_count++] = (byte) ((value >>> 8) & 0xFF);
        _buffer[_count++] = (byte) ((value >>> 0) & 0xFF);
    }

    public void writeLong(long value) {
        byte b = 0;
        if (value < 0L) {
            b = 64;
            value = ~value;
        }
        b |= (byte) ((int) value & 0x3f);
        for (value = (long) value >> 6; value != 0L; value = (long) (value) >> 7) {
            b |= 0x80;
            ensureCapacity(1);
            _buffer[_count++] = b;
            b = (byte) ((int) value & 0x7f);
        }

        ensureCapacity(1);
        _buffer[_count++] = b;
    }

    public void writeLongFixed(long value) {
        ensureCapacity(SIZE_LONG);
        _buffer[_count++] = (byte) (value >>> 56);
        _buffer[_count++] = (byte) (value >>> 48);
        _buffer[_count++] = (byte) (value >>> 40);
        _buffer[_count++] = (byte) (value >>> 32);
        _buffer[_count++] = (byte) (value >>> 24);
        _buffer[_count++] = (byte) (value >>> 16);
        _buffer[_count++] = (byte) (value >>> 8);
        _buffer[_count++] = (byte) (value >>> 0);
    }

    public void writeFloat(float value) {
        int rawValue = Float.floatToIntBits(value);
        writeInt(rawValue);
    }

    public void writeDouble(double value) {
        long rawValue = Double.doubleToLongBits(value);
        writeLong(rawValue);
    }

    public void writeBoolean(boolean value) {
        writeByte(value ? (byte) 1 : (byte) 0);
    }

    public void writeChar(char value) {
        writeInt(value);
    }

    public void writeString(String value) {
        // Write string type
        if (value == null) {
            writeByte(PbsStringType.NULL);
            return;
        }
        int length = value.length();
        if (length == 0) {
            writeByte(PbsStringType.EMPTY);
            return;
        }
        // Save position:
        int startingPosition = _count;
        // By default attempt to write the string in ASCII format
        writeByte(PbsStringType.ASCII);
        // Write length:
        writeInt(length);
        // Save position from where the string bytes starts
        int positionBeforeContent = _count;
        // Start by assuming string is not unicode (i.e. 1 byte per char):
        boolean isUnicode = false;
        // Used to indicate the ascii part length of the string
        int asciiIntervalLength = 0;
        // Get the string characters:
        char[] chars = value.toCharArray();
        // make sure buffer is big enough for 1 byte per char:
        ensureCapacity(length);
        // Write characters:
        for (int i = 0; i < length; i++) {
            char currChar = chars[i];
            // If character requires more than a byte, stop:
            if (currChar > 255) {
                isUnicode = true;
                asciiIntervalLength = i;
                // make sure buffer is big enough for 2 bytes per char:
                ensureCapacity((length * 2) - i);
                break;
            }
            _buffer[_count++] = (byte) currChar;
        }

        if (isUnicode) {
            // Go back to position before content:
            _count = startingPosition;
            // Switch to unicode format
            writeByte(PbsStringType.UNICODE);
            // Fix position to start after the already written chars:
            _count = positionBeforeContent + asciiIntervalLength;
            // Write characters:
            for (int i = asciiIntervalLength; i < length; i++, _count++) {
                char currChar = chars[i];
                // Write first byte:
                _buffer[_count] = (byte) currChar;
                // Write second byte at offset of length:
                _buffer[_count + length] = (byte) (currChar >> 8);
            }
            // Make sure there are zeros on the ascii chars
            for (int i = 0; i < asciiIntervalLength; i++)
                _buffer[_count++] = 0;
            // Fix position to point to the end of the string bytes:
            _count += length - asciiIntervalLength;
        }
    }

    public void writeRepetitiveString(String value) {
        // TODO implement
        writeString(value);
    }

    public void writeDateTime(Date value) {
        long rawValue = value.getTime();
        writeLong(rawValue);
    }

    public void writeDecimal(BigDecimal value) {
        String rawValue = value.toEngineeringString();
        writeString(rawValue);
    }

    public void writeUUID(UUID value) {
        ensureCapacity(SIZE_UUID);
        writeLongFixed(value.getLeastSignificantBits());
        writeLongFixed(value.getMostSignificantBits());
    }

    public void writeByteArray(byte[] array) {
        if (array == null)
            writeInt(-1);
        else {
            writeInt(array.length);
            write(array, 0, array.length);
        }
    }

    public void writeShortArray(short[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            this.writeShort(array[i]);
    }

    public void writeIntArray(int[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeInt(array[i]);
    }

    public void writeLongArray(long[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeLong(array[i]);
    }

    public void writeFloatArray(float[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeFloat(array[i]);
    }

    public void writeDoubleArray(double[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeDouble(array[i]);
    }

    public void writeBooleanArray(boolean[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeBoolean(array[i]);
    }

    public void writeCharArray(char[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeChar(array[i]);
    }

    public void writeStringArray(String[] array) {
        if (array == null)
            writeInt(-1);
        else {
            writeInt(array.length);
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null)
                    writeByte(PbsTypeInfo.NULL);
                else {
                    writeByte(PbsTypeInfo.STRING);
                    writeString(array[i]);
                }
            }
        }
    }

    public void writeDateTimeArray(Date[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.DATE);
                writeDateTime(array[i]);
            }
        }
    }

    public void writeDecimalArray(BigDecimal[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.DECIMAL);
                writeDecimal(array[i]);
            }
        }
    }

    public void writeUUIDArray(UUID[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.UUID);
                writeUUID(array[i]);
            }
        }
    }

    public void writeByteWrapperArray(Byte[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.BYTE_WRAPPER);
                writeByte(array[i]);
            }
        }
    }

    public void writeShortWrapperArray(Short[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.SHORT_WRAPPER);
                writeShort(array[i]);
            }
        }
    }

    public void writeIntegerWrapperArray(Integer[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.INTEGER_WRAPPER);
                writeInt(array[i]);
            }
        }
    }

    public void writeLongWrapperArray(Long[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.LONG_WRAPPER);
                writeLong(array[i]);
            }
        }
    }

    public void writeFloatWrapperArray(Float[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.FLOAT_WRAPPER);
                writeFloat(array[i]);
            }
        }
    }

    public void writeDoubleWrapperArray(Double[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.DOUBLE_WRAPPER);
                writeDouble(array[i]);
            }
        }
    }

    public void writeBooleanWrapperArray(Boolean[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.BOOLEAN_WRAPPER);
                writeBoolean(array[i]);
            }
        }
    }

    public void writeCharWrapperArray(Character[] array) {
        writeInt(array.length);
        for (int i = 0; i < array.length; i++) {
            if (array[i] == null)
                writeByte(PbsTypeInfo.NULL);
            else {
                writeByte(PbsTypeInfo.CHAR_WRAPPER);
                writeChar(array[i]);
            }
        }
    }

    public void writeProperties(Properties properties) {
        if (properties == null)
            writeInt(-1);
        else {
            writeInt(properties.size());
            for (Map.Entry prop : properties.entrySet()) {
                writeString(prop.getKey().toString());
                writeString(prop.getValue().toString());
            }
        }
    }

    public void writeBinaryObject(BinaryObject value) {
        // Write format code:
        writeByte(value.getFormatCode());
        // Write data length:
        byte[] data = value.getData();
        writeIntFixed(data.length);
        // Write data (NOTE: this specific overload gives best performance).
        write(data, 0, data.length);
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
