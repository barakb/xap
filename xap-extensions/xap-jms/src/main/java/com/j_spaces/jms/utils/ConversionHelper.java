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

package com.j_spaces.jms.utils;

import javax.jms.MessageFormatException;


/**
 * The <code>ConversionHelper</code> class is used for converting values carried by messages into
 * specified types, if possible.
 *
 * @author Gershon Diner
 * @version 4.0
 */
public class ConversionHelper {

    /**
     * Convert value to boolean
     *
     * @param value the object to convert from
     * @return the converted boolean
     * @throws MessageFormatException if the conversion is invalid
     */
    public static boolean getBoolean(Object value)
            throws MessageFormatException {
        boolean result = false;

        if (value instanceof Boolean) {
            result = ((Boolean) value).booleanValue();
        } else if (value instanceof String) {
            result = Boolean.valueOf((String) value).booleanValue();
        } else if (value == null) {
            result = Boolean.valueOf((String) value).booleanValue();
        } else {
            raise(value, boolean.class);
        }
        return result;
    }

    /**
     * Convert value to byte
     *
     * @param value the object to convert from
     * @return the converted byte
     * @throws MessageFormatException if the conversion is invalid
     * @throws NumberFormatException  if value is a String and the conversion is invalid
     */
    public static byte getByte(Object value) throws MessageFormatException {
        byte result = 0;
        if (value == null) {
//return 0;
            return Byte.valueOf(null);
        }
        if (value instanceof Byte) {
            result = ((Byte) value).byteValue();
        } else if (value instanceof String) {
            result = Byte.parseByte((String) value);
        } else {
            raise(value, byte.class);
        }
        return result;
    }


    /**
     * Convert value to short
     *
     * @param value the object to convert from
     * @return the converted short
     * @throws MessageFormatException if the conversion is invalid
     * @throws NumberFormatException  if value is a String and the conversion is invalid
     */
    public static short getShort(Object value) throws MessageFormatException {
        short result = 0;
        if (value == null) {
//return 0;
            return Short.valueOf(null);
        }
        if (value instanceof Short) {
            result = ((Short) value).shortValue();
        } else if (value instanceof Byte) {
            result = ((Byte) value).shortValue();
        } else if (value instanceof String) {
            result = Short.parseShort((String) value);
        } else {
            raise(value, short.class);
        }
        return result;
    }

    /**
     * Convert value to char
     *
     * @param value the object to convert from
     * @return the converted char
     * @throws MessageFormatException if the conversion is invalid
     * @throws NullPointerException   if value is null
     */
    public static char getChar(Object value) throws MessageFormatException {
        char result = '\0';
        if (value instanceof Character) {
            result = ((Character) value).charValue();
        } else if (value == null) {
            throw new NullPointerException("Cannot convert null value to char");
        } else {
            raise(value, char.class);
        }
        return result;
    }

    /**
     * Convert value to int
     *
     * @param value the object to convert from
     * @return the converted int
     * @throws MessageFormatException if the conversion is invalid
     * @throws NumberFormatException  if value is a String and the conversion is invalid
     */
    public static int getInt(Object value) throws MessageFormatException {
        int result = 0;
        if (value == null) {
//return 0;
            return Integer.valueOf(null);
        }

        if (value instanceof Integer) {
            result = ((Integer) value).intValue();
        } else if (value instanceof Short) {
            result = ((Short) value).intValue();
        } else if (value instanceof Byte) {
            result = ((Byte) value).intValue();
        } else if (value instanceof String) {
            result = Integer.parseInt((String) value);
        } else {
            raise(value, int.class);
        }
        return result;
    }


    /**
     * Convert value to long
     *
     * @param value the object to convert from
     * @return the converted long
     * @throws MessageFormatException if the conversion is invalid
     * @throws NumberFormatException  if value is a String and the conversion is invalid
     */
    public static long getLong(Object value) throws MessageFormatException {
        long result = 0;
        if (value == null) {
//return 0;
            return Long.valueOf(null);
        }
        if (value instanceof Long) {
            result = ((Long) value).longValue();
        } else if (value instanceof Integer) {
            result = ((Integer) value).longValue();
        } else if (value instanceof Short) {
            result = ((Short) value).longValue();
        } else if (value instanceof Byte) {
            result = ((Byte) value).longValue();
        } else if (value instanceof String) {
            result = Long.parseLong((String) value);
        } else {
            raise(value, long.class);
        }
        return result;
    }

    /**
     * Convert value to float
     *
     * @param value the object to convert from
     * @return the converted float
     * @throws MessageFormatException if the conversion is invalid
     * @throws NumberFormatException  if value is a String and the conversion is invalid
     */
    public static float getFloat(Object value) throws MessageFormatException {
        float result = 0;
        if (value == null) {
//return 0;
            return Float.valueOf(null);
        }
        if (value instanceof Float) {
            result = ((Float) value).floatValue();
        } else if (value instanceof String) {
            result = Float.parseFloat((String) value);
        } else {
            raise(value, float.class);
        }
        return result;
    }

    /**
     * Convert value to double
     *
     * @param value the object to convert from
     * @return the converted double
     * @throws MessageFormatException if the conversion is invalid
     * @throws NumberFormatException  if value is a String and the conversion is invalid
     */
    public static double getDouble(Object value) throws MessageFormatException {
        double result = 0;
        if (value == null) {
//return 0;
            return Double.valueOf(null);
        }
        if (value instanceof Double) {
            result = ((Double) value).doubleValue();
        } else if (value instanceof Float) {
            result = ((Float) value).doubleValue();
        } else if (value instanceof String) {
            result = Double.parseDouble((String) value);
        } else {
            raise(value, double.class);
        }
        return result;
    }

    /**
     * Convert value to String
     *
     * @param value the object to convert from
     * @return the converted String
     * @throws MessageFormatException if the conversion is invalid
     */
    public static String getString(Object value) throws MessageFormatException {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            raise(value, String.class);
        }
        return String.valueOf(value);
    }

    /**
     * Convert value to byte[]
     *
     * @param value the object to convert from. This must be a byte array, or null
     * @return a copy of the supplied array, or null
     * @throws MessageFormatException if value is not a byte array or is not null
     */
    public static byte[] getBytes(Object value)
            throws MessageFormatException {
        byte[] result = null;

        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            result = new byte[bytes.length];
            System.arraycopy(bytes, 0, result, 0, bytes.length);
        } else if (value != null) {
            raise(value, byte[].class);
        }
        return result;
    }

    /**
     * Helper to raise a MessageFormatException when a conversion cannot be performed
     *
     * @param value the value that cannot be converted
     * @param type  the type that the value cannot be converted to
     * @throws MessageFormatException when invoked
     */
    private static void raise(Object value, Class type)
            throws MessageFormatException {

        throw new MessageFormatException(
                "Cannot convert values of type " + value.getClass().getName() +
                        " to " + type.getName());
    }

}
