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

package com.gigaspaces.internal.utils;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Miscellaneous object utility methods. Mainly for internal use within the framework; consider
 * Jakarta's Commons Lang for a more comprehensive suite of object utilities.
 *
 * @author kimchy
 */
public abstract class ObjectUtils {

    private static final int INITIAL_HASH = 7;
    private static final int MULTIPLIER = 31;

    private static final String EMPTY_STRING = "";
    private static final String NULL_STRING = "null";
    private static final String ARRAY_START = "{";
    private static final String ARRAY_END = "}";
    private static final String EMPTY_ARRAY = ARRAY_START + ARRAY_END;
    private static final String ARRAY_ELEMENT_SEPARATOR = ", ";

    private static final Map<String, Class<?>> _primitiveTypes = initPrimitiveTypes();
    private static final Map<Class<?>, Object> _defaultValues = initDefaultValues();

    private static Map<String, Class<?>> initPrimitiveTypes() {
        Map<String, Class<?>> result = new HashMap<String, Class<?>>();
        result.put(byte.class.getName(), byte.class);
        result.put(short.class.getName(), short.class);
        result.put(int.class.getName(), int.class);
        result.put(long.class.getName(), long.class);
        result.put(float.class.getName(), float.class);
        result.put(double.class.getName(), double.class);
        result.put(boolean.class.getName(), boolean.class);
        result.put(char.class.getName(), char.class);
        return result;
    }

    private static Map<Class<?>, Object> initDefaultValues() {
        Map<Class<?>, Object> result = new HashMap<Class<?>, Object>();
        result.put(byte.class, (byte) 0);
        result.put(short.class, (short) 0);
        result.put(int.class, (int) 0);
        result.put(long.class, (long) 0);
        result.put(float.class, (float) 0.0);
        result.put(double.class, (double) 0.0);
        result.put(boolean.class, false);
        result.put(char.class, (char) 0);
        return result;
    }

    /**
     * Return whether the given throwable is a checked exception: that is, neither a
     * RuntimeException nor an Error.
     *
     * @param ex the throwable to check
     * @return whether the throwable is a checked exception
     * @see java.lang.Exception
     * @see java.lang.RuntimeException
     * @see java.lang.Error
     */
    public static boolean isCheckedException(Throwable ex) {
        return !(ex instanceof RuntimeException || ex instanceof Error);
    }

    /**
     * Check whether the given exception is compatible with the exceptions declared in a throws
     * clause.
     *
     * @param ex                 the exception to checked
     * @param declaredExceptions the exceptions declared in the throws clause
     * @return whether the given exception is compatible
     */
    public static boolean isCompatibleWithThrowsClause(Throwable ex, Class<?>[] declaredExceptions) {
        if (!isCheckedException(ex))
            return true;

        if (declaredExceptions != null)
            for (Class<?> declaredException : declaredExceptions)
                if (declaredException.isAssignableFrom(ex.getClass()))
                    return true;

        return false;
    }

    /**
     * Return whether the given array is empty: that is, <code>null</code> or of zero length.
     *
     * @param array the array to check
     */
    public static boolean isEmpty(Object[] array) {
        return (array == null || array.length == 0);
    }

    /**
     * Append the given Object to the given array, returning a new array consisting of the input
     * array contents plus the given Object.
     *
     * @param array the array to append to (can be <code>null</code>)
     * @param obj   the Object to append
     * @return the new array (of the same component type; never <code>null</code>)
     */
    public static <T> T[] addObjectToArray(T[] array, T obj) {
        Class<?> compType;

        if (array != null)
            compType = array.getClass().getComponentType();
        else if (obj != null)
            compType = obj.getClass();
        else
            compType = Object.class;

        int newArrLength = (array != null ? array.length + 1 : 1);

        T[] newArr = (T[]) Array.newInstance(compType, newArrLength);
        if (array != null)
            System.arraycopy(array, 0, newArr, 0, array.length);

        newArr[newArr.length - 1] = obj;
        return newArr;
    }

    //---------------------------------------------------------------------
    // Convenience methods for content-based equality/hash-code handling
    //---------------------------------------------------------------------

    public static int hashCode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    public static boolean equals(Object o1, Object o2) {
        if (o1 == o2)
            return true;
        if (o1 == null || o2 == null)
            return false;

        return o1.equals(o2);
    }

    /**
     * Determine if the given objects are equal, returning <code>true</code> if both are
     * <code>null</code> or <code>false</code> if only one is <code>null</code>. <p>Compares arrays
     * with <code>Arrays.equals</code>, performing an equality check based on the array elements
     * rather than the array reference.
     *
     * @param o1 first Object to compare
     * @param o2 second Object to compare
     * @return whether the given objects are equal
     * @see java.util.Arrays#equals
     */
    public static boolean nullSafeEquals(Object o1, Object o2) {
        // Check if same:
        if (o1 == o2)
            return true;
        // Check if either is null:
        if (o1 == null || o2 == null)
            return false;
        // Check if equals:
        if (o1.equals(o2))
            return true;

        // If array, check array equality:
        if (o1 instanceof Object[] && o2 instanceof Object[])
            return Arrays.equals((Object[]) o1, (Object[]) o2);

        // If primitive array, check array equality:
        if (o1 instanceof byte[] && o2 instanceof byte[])
            return Arrays.equals((byte[]) o1, (byte[]) o2);
        if (o1 instanceof short[] && o2 instanceof short[])
            return Arrays.equals((short[]) o1, (short[]) o2);
        if (o1 instanceof int[] && o2 instanceof int[])
            return Arrays.equals((int[]) o1, (int[]) o2);
        if (o1 instanceof long[] && o2 instanceof long[])
            return Arrays.equals((long[]) o1, (long[]) o2);
        if (o1 instanceof float[] && o2 instanceof float[])
            return Arrays.equals((float[]) o1, (float[]) o2);
        if (o1 instanceof double[] && o2 instanceof double[])
            return Arrays.equals((double[]) o1, (double[]) o2);
        if (o1 instanceof boolean[] && o2 instanceof boolean[])
            return Arrays.equals((boolean[]) o1, (boolean[]) o2);
        if (o1 instanceof char[] && o2 instanceof char[])
            return Arrays.equals((char[]) o1, (char[]) o2);

        return false;
    }

    /**
     * Return as hash code for the given object; typically the value of <code>{@link
     * Object#hashCode()}</code>. If the object is an array, this method will delegate to any of the
     * <code>nullSafeHashCode</code> methods for arrays in this class. If the object is
     * <code>null</code>, this method returns 0.
     *
     * @see #nullSafeHashCode(Object[])
     * @see #nullSafeHashCode(boolean[])
     * @see #nullSafeHashCode(byte[])
     * @see #nullSafeHashCode(char[])
     * @see #nullSafeHashCode(double[])
     * @see #nullSafeHashCode(float[])
     * @see #nullSafeHashCode(int[])
     * @see #nullSafeHashCode(long[])
     * @see #nullSafeHashCode(short[])
     */
    public static int nullSafeHashCode(Object obj) {
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Object[]) {
            return nullSafeHashCode((Object[]) obj);
        }
        if (obj instanceof boolean[]) {
            return nullSafeHashCode((boolean[]) obj);
        }
        if (obj instanceof byte[]) {
            return nullSafeHashCode((byte[]) obj);
        }
        if (obj instanceof char[]) {
            return nullSafeHashCode((char[]) obj);
        }
        if (obj instanceof double[]) {
            return nullSafeHashCode((double[]) obj);
        }
        if (obj instanceof float[]) {
            return nullSafeHashCode((float[]) obj);
        }
        if (obj instanceof int[]) {
            return nullSafeHashCode((int[]) obj);
        }
        if (obj instanceof long[]) {
            return nullSafeHashCode((long[]) obj);
        }
        if (obj instanceof short[]) {
            return nullSafeHashCode((short[]) obj);
        }
        return obj.hashCode();
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(Object[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + nullSafeHashCode(array[i]);
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(boolean[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + hashCode(array[i]);
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(byte[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + array[i];
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(char[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + array[i];
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(double[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + hashCode(array[i]);
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(float[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + hashCode(array[i]);
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(int[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + array[i];
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(long[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + hashCode(array[i]);
        }
        return hash;
    }

    /**
     * Return a hash code based on the contents of the specified array. If <code>array</code> is
     * <code>null</code>, this method returns 0.
     */
    public static int nullSafeHashCode(short[] array) {
        if (array == null) {
            return 0;
        }
        int hash = INITIAL_HASH;
        int arraySize = array.length;
        for (int i = 0; i < arraySize; i++) {
            hash = MULTIPLIER * hash + array[i];
        }
        return hash;
    }

    /**
     * Return the same value as <code>{@link Boolean#hashCode()}</code>.
     *
     * @see Boolean#hashCode()
     */
    public static int hashCode(boolean bool) {
        return bool ? 1231 : 1237;
    }

    /**
     * Return the same value as <code>{@link Double#hashCode()}</code>.
     *
     * @see Double#hashCode()
     */
    public static int hashCode(double dbl) {
        long bits = Double.doubleToLongBits(dbl);
        return hashCode(bits);
    }

    /**
     * Return the same value as <code>{@link Float#hashCode()}</code>.
     *
     * @see Float#hashCode()
     */
    public static int hashCode(float flt) {
        return Float.floatToIntBits(flt);
    }

    /**
     * Return the same value as <code>{@link Long#hashCode()}</code>.
     *
     * @see Long#hashCode()
     */
    public static int hashCode(long lng) {
        return (int) (lng ^ (lng >>> 32));
    }

    //---------------------------------------------------------------------
    // Convenience methods for content-based toString output
    //---------------------------------------------------------------------

    /**
     * Return a String representation of the specified Object. <p>Returns {@link #NULL_STRING the
     * String 'null'} if <code>obj</code> is <code>null</code>.
     *
     * @param obj the object whose String representation to return
     * @return a String representation of <code>obj</code>
     */
    public static String nullSafeToString(Object obj) {
        if (obj == null) {
            return NULL_STRING;
        }
        if (obj instanceof String) {
            return (String) obj;
        }
        if (obj instanceof Object[]) {
            return nullSafeToString((Object[]) obj);
        }
        if (obj instanceof boolean[]) {
            return nullSafeToString((boolean[]) obj);
        }
        if (obj instanceof byte[]) {
            return nullSafeToString((byte[]) obj);
        }
        if (obj instanceof char[]) {
            return nullSafeToString((char[]) obj);
        }
        if (obj instanceof double[]) {
            return nullSafeToString((double[]) obj);
        }
        if (obj instanceof float[]) {
            return nullSafeToString((float[]) obj);
        }
        if (obj instanceof int[]) {
            return nullSafeToString((int[]) obj);
        }
        if (obj instanceof long[]) {
            return nullSafeToString((long[]) obj);
        }
        if (obj instanceof short[]) {
            return nullSafeToString((short[]) obj);
        }
        return obj.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(Object[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }
            buffer.append(String.valueOf(array[i]));
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(boolean[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }

            buffer.append(array[i]);
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(byte[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }
            buffer.append(array[i]);
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(char[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }
            buffer.append("'").append(array[i]).append("'");
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(double[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }

            buffer.append(array[i]);
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(float[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }

            buffer.append(array[i]);
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(int[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }
            buffer.append(array[i]);
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(long[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }
            buffer.append(array[i]);
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of the contents of the specified array. The String
     * representation consists of a list of the array's elements, enclosed in curly braces
     * (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code> (a
     * comma followed by a space). Returns <code>"null"</code> if <code>array</code> is
     * <code>null</code>.
     *
     * @param array the array whose String representation to return
     * @return a String representation of <code>array</code>
     */
    public static String nullSafeToString(short[] array) {
        if (array == null) {
            return NULL_STRING;
        }
        int length = array.length;
        if (length == 0) {
            return EMPTY_ARRAY;
        }
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                buffer.append(ARRAY_START);
            } else {
                buffer.append(ARRAY_ELEMENT_SEPARATOR);
            }
            buffer.append(array[i]);
        }
        buffer.append(ARRAY_END);
        return buffer.toString();
    }

    /**
     * Return a String representation of an object's overall identity.
     *
     * @param obj the object (may be <code>null</code>)
     * @return the object's identity as String representation, or <code>null</code> if the object
     * was <code>null</code>
     */
    public static String identityToString(Object obj) {
        if (obj == null) {
            return EMPTY_STRING;
        }
        return obj.getClass().getName() + "@" + getIdentityHexString(obj);
    }

    /**
     * Return a hex String form of an object's identity hash code.
     *
     * @param obj the object
     * @return the object's identity code in hex notation
     */
    public static String getIdentityHexString(Object obj) {
        return Integer.toHexString(System.identityHashCode(obj));
    }

    /**
     * Return a content-based String representation if <code>obj</code> is not <code>null</code>;
     * otherwise returns an empty <code>String</code>. <p>Differs from <code>nullSafeToString</code>
     * in that it returns an empty String rather than "null" for a <code>null</code> value.
     *
     * @see #nullSafeToString(Object)
     */
    public static String getDisplayString(Object obj) {
        if (obj == null) {
            return EMPTY_STRING;
        }
        return nullSafeToString(obj);
    }

    /**
     * Returns whether the specified type name is of primitive type
     */
    public static boolean isPrimitive(String typeName) {
        return _primitiveTypes.containsKey(typeName);
    }

    /**
     * Gets the class represeting the primitive type denoted by 'name' if 'name' denotes a primitive
     * and null otherwise.
     */
    public static Class<?> getPrimitive(String typeName) {
        return _primitiveTypes.get(typeName);
    }

    /**
     * Returns null for reference types and a boxed default value for primitive types
     */
    public static Object getDefaultValue(Class<?> type) {
        return _defaultValues.get(type);
    }

    /**
     * Throws an IllegalArgumentException if the provided argument is null.
     *
     * @param <T>          The argument type.
     * @param argument     The argument instance to check.
     * @param argumentName The argument name (for thrown Exception message).
     * @return Returns the provided argument if exception wasn't thrown
     */
    public static <T> T assertArgumentNotNull(T argument, String argumentName) {
        if (argument == null)
            throw new IllegalArgumentException("Argument cannot be null - '" + argumentName + "'");
        return argument;
    }
}