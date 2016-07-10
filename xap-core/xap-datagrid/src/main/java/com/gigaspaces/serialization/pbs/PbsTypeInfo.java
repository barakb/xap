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

import com.gigaspaces.serialization.pbs.collections.PbsCustomTypeList;
import com.gigaspaces.serialization.pbs.collections.PbsCustomTypeMap;
import com.j_spaces.core.exception.internal.PBSInternalSpaceException;

import java.util.HashMap;

/**
 * @author Niv
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class PbsTypeInfo {
    public static final byte NULL = 127;

    // Primitive types
    public static final byte BYTE = 7;
    public static final byte SHORT = 9;
    public static final byte INTEGER = 2;
    public static final byte LONG = 8;
    public static final byte FLOAT = 3;
    public static final byte DOUBLE = 4;
    public static final byte BOOLEAN = 6;
    public static final byte CHAR = 5;
    // Primitive type wrappers
    public static final byte BYTE_WRAPPER = 11;
    public static final byte SHORT_WRAPPER = 12;
    public static final byte INTEGER_WRAPPER = 13;
    public static final byte LONG_WRAPPER = 14;
    public static final byte FLOAT_WRAPPER = 15;
    public static final byte DOUBLE_WRAPPER = 16;
    public static final byte BOOLEAN_WRAPPER = 17;
    public static final byte CHAR_WRAPPER = 18;
    // System Types
    public static final byte STRING = 1;
    public static final byte DATE = 22;
    public static final byte DECIMAL = 23;
    public static final byte UUID = 24;

    // Array
    public static final byte ARRAY = 31;
    // Object
    public static final byte OBJECT = 48;
    // Binary data container
    public static final byte BINARY = 49;

    // public static final byte PBS_OBJECT_ARRAY = 62;

    // Hard-coded arrays:
    public static final byte BYTE_ARRAY = 56;
    public static final byte SHORT_ARRAY = 58;
    public static final byte INTEGER_ARRAY = 51;
    public static final byte LONG_ARRAY = 57;
    public static final byte FLOAT_ARRAY = 52;
    public static final byte DOUBLE_ARRAY = 53;
    public static final byte BOOLEAN_ARRAY = 55;
    public static final byte CHAR_ARRAY = 54;
    public static final byte STRING_ARRAY = 50;

    public static final byte LIST_CUSTOM_TYPE = 70;
    //public static final byte LIST_CUSTOM		= 71;
    //public static final byte LIST_INTERFACE	= 72;
    public static final byte LIST_ARRAY = 73;
    //public static final byte LIST_LINKED		= 74;
    //public static final byte LIST_STACK		= 75;

    public static final byte MAP_CUSTOM_TYPE = 80;
    //public static final byte MAP_CUSTOM		= 81;
    //public static final byte MAP_INTERFACE	= 82;
    public static final byte MAP_HASH = 83;
    public static final byte MAP_TREE = 84;
    public static final byte MAP_PROPERTIES = 85;
    public static final byte MAP_LINKED = 86;
    public static final byte DOCUMENT_PROPERTIES = 87;

    public static final byte DOCUMENT = 88;

    //public static final byte LIST_INTERFACE_GENERIC = 89;
    public static final byte LIST_ARRAY_GENERIC = 90;
    //public static final byte MAP_INTERFACE_GENERIC = 91;
    public static final byte MAP_HASH_GENERIC = 92;
    public static final byte MAP_TREE_GENERIC = 93;
    public static final byte MAP_LINKED_GENERIC = 94;

    public final byte typeCode;
    public final PbsTypeInfo componentType;

    private PbsTypeInfo(byte typeCode) {
        this.typeCode = typeCode;
        this.componentType = null;
    }

    private PbsTypeInfo(byte typeCode, PbsTypeInfo componentType) {
        this.typeCode = typeCode;
        this.componentType = componentType;
    }

    private static PbsTypeInfo PbsArray(PbsTypeInfo componentType) {
        return new PbsTypeInfo(PbsTypeInfo.ARRAY, componentType);
    }

    // Special types:
    private static final PbsTypeInfo NullInfo = new PbsTypeInfo(PbsTypeInfo.NULL);
    private static final PbsTypeInfo ObjectInfo = new PbsTypeInfo(OBJECT);
    private static final PbsTypeInfo DocumentInfo = new PbsTypeInfo(DOCUMENT);
    private static final PbsTypeInfo ObjectArrayInfo = PbsArray(ObjectInfo);
    private static final PbsTypeInfo BinaryInfo = new PbsTypeInfo(BINARY);

    // Primitive types:
    private static final PbsTypeInfo ByteInfo = new PbsTypeInfo(BYTE);
    private static final PbsTypeInfo ShortInfo = new PbsTypeInfo(SHORT);
    private static final PbsTypeInfo IntegerInfo = new PbsTypeInfo(INTEGER);
    private static final PbsTypeInfo LongInfo = new PbsTypeInfo(LONG);
    private static final PbsTypeInfo FloatInfo = new PbsTypeInfo(FLOAT);
    private static final PbsTypeInfo DoubleInfo = new PbsTypeInfo(DOUBLE);
    private static final PbsTypeInfo BooleanInfo = new PbsTypeInfo(BOOLEAN);
    private static final PbsTypeInfo CharInfo = new PbsTypeInfo(CHAR);
    // Primitive wrapper types:
    private static final PbsTypeInfo ByteWrapperInfo = new PbsTypeInfo(BYTE_WRAPPER);
    private static final PbsTypeInfo ShortWrapperInfo = new PbsTypeInfo(SHORT_WRAPPER);
    private static final PbsTypeInfo IntegerWrapperInfo = new PbsTypeInfo(INTEGER_WRAPPER);
    private static final PbsTypeInfo LongWrapperInfo = new PbsTypeInfo(LONG_WRAPPER);
    private static final PbsTypeInfo FloatWrapperInfo = new PbsTypeInfo(FLOAT_WRAPPER);
    private static final PbsTypeInfo DoubleWrapperInfo = new PbsTypeInfo(DOUBLE_WRAPPER);
    private static final PbsTypeInfo BooleanWrapperInfo = new PbsTypeInfo(BOOLEAN_WRAPPER);
    private static final PbsTypeInfo CharWrapperInfo = new PbsTypeInfo(CHAR_WRAPPER);
    // System types:
    private static final PbsTypeInfo StringInfo = new PbsTypeInfo(STRING);
    private static final PbsTypeInfo DateInfo = new PbsTypeInfo(DATE);
    private static final PbsTypeInfo DecimalInfo = new PbsTypeInfo(DECIMAL);
    private static final PbsTypeInfo UUIDInfo = new PbsTypeInfo(UUID);
    // hard-coded arrays:
    private static final PbsTypeInfo ByteArrayInfo = new PbsTypeInfo(BYTE_ARRAY);
    private static final PbsTypeInfo ShortArrayInfo = new PbsTypeInfo(SHORT_ARRAY);
    private static final PbsTypeInfo IntegerArrayInfo = new PbsTypeInfo(INTEGER_ARRAY);
    private static final PbsTypeInfo LongArrayInfo = new PbsTypeInfo(LONG_ARRAY);
    private static final PbsTypeInfo FloatArrayInfo = new PbsTypeInfo(FLOAT_ARRAY);
    private static final PbsTypeInfo DoubleArrayInfo = new PbsTypeInfo(DOUBLE_ARRAY);
    private static final PbsTypeInfo BooleanArrayInfo = new PbsTypeInfo(BOOLEAN_ARRAY);
    private static final PbsTypeInfo CharArrayInfo = new PbsTypeInfo(CHAR_ARRAY);
    private static final PbsTypeInfo StringArrayInfo = new PbsTypeInfo(STRING_ARRAY);

    private static final HashMap<Class<?>, PbsTypeInfo> typeInfoMap = loadTypeInfoMap();

    private static final HashMap<Class<?>, PbsTypeInfo> loadTypeInfoMap() {
        HashMap<Class<?>, PbsTypeInfo> map = new HashMap<Class<?>, PbsTypeInfo>(50);

        // Primitive types cannot exist in the space, so they're not mapped.
        // Map primitive types wrappers as primitive types:
        map.put(java.lang.Byte.class, ByteInfo);
        map.put(java.lang.Short.class, ShortInfo);
        map.put(java.lang.Integer.class, IntegerInfo);
        map.put(java.lang.Long.class, LongInfo);
        map.put(java.lang.Float.class, FloatInfo);
        map.put(java.lang.Double.class, DoubleInfo);
        map.put(java.lang.Boolean.class, BooleanInfo);
        map.put(java.lang.Character.class, CharInfo);
        // Map system types:
        map.put(java.lang.String.class, StringInfo);
        map.put(java.util.Date.class, DateInfo);
        map.put(java.math.BigDecimal.class, DecimalInfo);
        map.put(java.util.UUID.class, UUIDInfo);

        // Map special types:
        map.put(java.lang.Object.class, ObjectInfo);
        map.put(com.gigaspaces.document.SpaceDocument.class, DocumentInfo);
        map.put(java.lang.Object[].class, ObjectArrayInfo);
        map.put(com.gigaspaces.serialization.BinaryObject.class, BinaryInfo);

        // Map Hard coded arrays:
        map.put(byte[].class, ByteArrayInfo);
        map.put(short[].class, ShortArrayInfo);
        map.put(int[].class, IntegerArrayInfo);
        map.put(long[].class, LongArrayInfo);
        map.put(float[].class, FloatArrayInfo);
        map.put(double[].class, DoubleArrayInfo);
        map.put(boolean[].class, BooleanArrayInfo);
        map.put(char[].class, CharArrayInfo);
        map.put(String[].class, StringArrayInfo);
        // Map Common arrays:
        //map.put(byte[].class, PbsArray(ByteInfo));
        //map.put(short[].class, PbsArray(ShortInfo));
        //map.put(int[].class, PbsArray(IntegerInfo));
        //map.put(long[].class, PbsArray(LongInfo));
        //map.put(float[].class, PbsArray(FloatInfo));
        //map.put(double[].class, PbsArray(DoubleInfo));
        //map.put(boolean[].class, PbsArray(BooleanInfo));
        //map.put(char[].class, PbsArray(CharInfo));
        map.put(Byte[].class, PbsArray(ByteWrapperInfo));
        map.put(Short[].class, PbsArray(ShortWrapperInfo));
        map.put(Integer[].class, PbsArray(IntegerWrapperInfo));
        map.put(Long[].class, PbsArray(LongWrapperInfo));
        map.put(Float[].class, PbsArray(FloatWrapperInfo));
        map.put(Double[].class, PbsArray(DoubleWrapperInfo));
        map.put(Boolean[].class, PbsArray(BooleanWrapperInfo));
        map.put(Character[].class, PbsArray(CharWrapperInfo));
        //map.put(String[].class, PbsArray(StringInfo));
        map.put(java.util.Date[].class, PbsArray(DateInfo));
        map.put(java.math.BigDecimal[].class, PbsArray(DecimalInfo));
        map.put(java.util.UUID[].class, PbsArray(UUIDInfo));
        map.put(com.gigaspaces.document.SpaceDocument[].class, PbsArray(DocumentInfo));

        // Lists:
        map.put(PbsCustomTypeList.class, new PbsTypeInfo(LIST_CUSTOM_TYPE));
        map.put(java.util.ArrayList.class, new PbsTypeInfo(LIST_ARRAY));
        //map.put(java.util.LinkedList.class, new PbsTypeInfo(LIST_LINKED));
        //map.put(java.util.Stack.class, new PbsTypeInfo(LIST_STACK));
        map.put(DotnetGenericArrayList.class, new PbsTypeInfo(LIST_ARRAY_GENERIC));

        // Maps:
        map.put(PbsCustomTypeMap.class, new PbsTypeInfo(MAP_CUSTOM_TYPE));
        map.put(java.util.HashMap.class, new PbsTypeInfo(MAP_HASH));
        map.put(java.util.LinkedHashMap.class, new PbsTypeInfo(MAP_LINKED));
        map.put(java.util.Properties.class, new PbsTypeInfo(MAP_PROPERTIES));
        map.put(java.util.TreeMap.class, new PbsTypeInfo(MAP_TREE));
        map.put(DotnetGenericHashMap.class, new PbsTypeInfo(MAP_HASH_GENERIC));
        map.put(DotnetGenericLinkedHashMap.class, new PbsTypeInfo(MAP_LINKED_GENERIC));
        map.put(DotnetGenericTreeMap.class, new PbsTypeInfo(MAP_TREE_GENERIC));

        map.put(com.gigaspaces.document.DocumentProperties.class, new PbsTypeInfo(DOCUMENT_PROPERTIES));

        return map;
    }

    public static PbsTypeInfo getTypeInfo(Object value) {
        if (value == null)
            return NullInfo;

        PbsTypeInfo typeInfo = typeInfoMap.get(value.getClass());

        if (typeInfo != null)
            return typeInfo;

        if (value instanceof Object[]) // Test for Array of UDT (User Defined Types)
            return ObjectArrayInfo;

        return ObjectInfo;
    }

    public static PbsTypeInfo getTypeInfo(byte typeCode) {
        switch (typeCode) {
            case NULL:
                return NullInfo;
            // Primitives:
            case BYTE:
                return ByteInfo;
            case SHORT:
                return ShortInfo;
            case INTEGER:
                return IntegerInfo;
            case LONG:
                return LongInfo;
            case FLOAT:
                return FloatInfo;
            case DOUBLE:
                return DoubleInfo;
            case BOOLEAN:
                return BooleanInfo;
            case CHAR:
                return CharInfo;
            // System types:
            case STRING:
                return StringInfo;
            case DATE:
                return DateInfo;
            case DECIMAL:
                return DecimalInfo;
            case UUID:
                return UUIDInfo;
            // Special types:
            case BINARY:
                return BinaryInfo;
            case OBJECT:
                return ObjectInfo;
            // Hard-coded arrays:
            case BYTE_ARRAY:
                return ByteArrayInfo;
            case SHORT_ARRAY:
                return ShortArrayInfo;
            case INTEGER_ARRAY:
                return IntegerArrayInfo;
            case LONG_ARRAY:
                return LongArrayInfo;
            case FLOAT_ARRAY:
                return FloatArrayInfo;
            case DOUBLE_ARRAY:
                return DoubleArrayInfo;
            case BOOLEAN_ARRAY:
                return BooleanArrayInfo;
            case CHAR_ARRAY:
                return CharArrayInfo;
            case STRING_ARRAY:
                return StringArrayInfo;
            // Others:
            default:
                throw new PBSInternalSpaceException("PBS failed to find type info by code: " + typeCode);
        }
    }
}