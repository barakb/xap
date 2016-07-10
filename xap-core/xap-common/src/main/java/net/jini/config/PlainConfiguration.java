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
package net.jini.config;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;

/**
 */
@com.gigaspaces.api.InternalApi
public class PlainConfiguration extends AbstractConfiguration {

    final private HashMap entries = new HashMap();

    protected Object getEntryInternal(String component, String name, Class type, Object data) throws ConfigurationException {
        Object entry = entries.get(component + '.' + name);
        if (entry == null) {
            String entrySysProperty = System.getProperty(component + "." + name);
            if (entrySysProperty != null) {
                if (type.equals(String.class)) {
                    return entrySysProperty;
                }
                Object number = parseNumber(entrySysProperty, type);
                if (number != null) {
                    return number;
                }
                throw new NoSuchEntryException("Entry for component [" + component + "] and name [" + name + "] can't be parsed, can't handle type [" + type.getName() + "]");
            }
            throw new NoSuchEntryException("No entry for component [" + component + "] and name [" + name + "]");
        }
        if (entry instanceof ConfigurationEntryFactory) {
            entry = ((ConfigurationEntryFactory) entry).create();
        }
        if (type.equals(int.class) || type.equals(long.class) || type.equals(float.class) || type.equals(boolean.class)
                || type.equals(short.class) || type.equals(double.class)) {
            return new Primitive(entry);
        }
        return entry;
    }

    public void setEntry(String component, String name, Object entry) {
        entries.put(component + '.' + name, entry);
    }

    public static Object parseNumber(String text, Class targetClass) {
        String trimmed = trimAllWhitespace(text);

        if (targetClass.equals(Byte.class)) {
            return (isHexNumber(trimmed) ? Byte.decode(trimmed) : Byte.valueOf(trimmed));
        } else if (targetClass.equals(boolean.class)) {
            return new Primitive(isHexNumber(trimmed) ? Byte.decode(trimmed) : Byte.valueOf(trimmed));
        } else if (targetClass.equals(Short.class)) {
            return (isHexNumber(trimmed) ? Short.decode(trimmed) : Short.valueOf(trimmed));
        } else if (targetClass.equals(short.class)) {
            return new Primitive(isHexNumber(trimmed) ? Short.decode(trimmed) : Short.valueOf(trimmed));
        } else if (targetClass.equals(Integer.class)) {
            return (isHexNumber(trimmed) ? Integer.decode(trimmed) : Integer.valueOf(trimmed));
        } else if (targetClass.equals(int.class)) {
            return new Primitive(isHexNumber(trimmed) ? Integer.decode(trimmed) : Integer.valueOf(trimmed));
        } else if (targetClass.equals(Long.class)) {
            return (isHexNumber(trimmed) ? Long.decode(trimmed) : Long.valueOf(trimmed));
        } else if (targetClass.equals(long.class)) {
            return new Primitive(isHexNumber(trimmed) ? Long.decode(trimmed) : Long.valueOf(trimmed));
        } else if (targetClass.equals(BigInteger.class)) {
            return (isHexNumber(trimmed) ? decodeBigInteger(trimmed) : new BigInteger(trimmed));
        } else if (targetClass.equals(Float.class)) {
            return Float.valueOf(trimmed);
        } else if (targetClass.equals(float.class)) {
            return new Primitive(Float.valueOf(trimmed));
        } else if (targetClass.equals(Double.class)) {
            return Double.valueOf(trimmed);
        } else if (targetClass.equals(double.class)) {
            return new Primitive(Double.valueOf(trimmed));
        } else if (targetClass.equals(BigDecimal.class) || targetClass.equals(Number.class)) {
            return new BigDecimal(trimmed);
        } else {
            return null;
        }
    }

    private static boolean isHexNumber(String value) {
        int index = (value.startsWith("-") ? 1 : 0);
        return (value.startsWith("0x", index) || value.startsWith("0X", index) || value.startsWith("#", index));
    }

    private static BigInteger decodeBigInteger(String value) {
        int radix = 10;
        int index = 0;
        boolean negative = false;

        // Handle minus sign, if present.
        if (value.startsWith("-")) {
            negative = true;
            index++;
        }

        // Handle radix specifier, if present.
        if (value.startsWith("0x", index) || value.startsWith("0X", index)) {
            index += 2;
            radix = 16;
        } else if (value.startsWith("#", index)) {
            index++;
            radix = 16;
        } else if (value.startsWith("0", index) && value.length() > 1 + index) {
            index++;
            radix = 8;
        }

        BigInteger result = new BigInteger(value.substring(index), radix);
        return (negative ? result.negate() : result);
    }

    public static String trimAllWhitespace(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        StringBuffer buf = new StringBuffer(str);
        int index = 0;
        while (buf.length() > index) {
            if (Character.isWhitespace(buf.charAt(index))) {
                buf.deleteCharAt(index);
            } else {
                index++;
            }
        }
        return buf.toString();
    }
}
