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

package com.gigaspaces.persistency.metadata;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.reflection.ISetterMethod;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.persistency.error.SpaceMongoException;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class SpaceDocumentMapperImpl implements SpaceDocumentMapper<DBObject> {

    private static final String _ID = "_id";
    private static final String TYPE = "__type__";
    private static final String VALUE = "__value__";
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
    private static final byte TYPE_ARRAY = Byte.MAX_VALUE - 1;
    private static final byte TYPE_COLLECTION = Byte.MAX_VALUE - 2;
    private static final byte TYPE_MAP = Byte.MAX_VALUE - 3;
    private static final byte TYPE_ENUM = Byte.MAX_VALUE - 4;
    private static final byte TYPE_OBJECTID = Byte.MAX_VALUE - 5;
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
        typeCodes.put(ObjectId.class, TYPE_OBJECTID);

    }

    private PojoRepository repository = new PojoRepository();
    private SpaceTypeDescriptor spaceTypeDescriptor;

    public SpaceDocumentMapperImpl(SpaceTypeDescriptor spaceTypeDescriptor) {
        this.spaceTypeDescriptor = spaceTypeDescriptor;
    }

    private byte type(Object value) {
        Byte type = typeCodes.get((value instanceof Class<?>) ? value : value
                .getClass());
        if (type == null) {
            if (value.getClass().isEnum())
                type = TYPE_ENUM;
            else if (value.getClass().isArray())
                type = TYPE_ARRAY;
            else if (Collection.class.isInstance(value))
                type = TYPE_COLLECTION;
            else if (Map.class.isInstance(value))
                type = TYPE_MAP;
            else
                type = TYPE_OBJECT;
        }
        return type;
    }

    private byte bsonType(Object value) {

        Byte type = typeCodes.get(value.getClass());

        if (type == null) {
            if (BasicDBList.class.isInstance(value))
                type = TYPE_ARRAY;
            else
                type = TYPE_OBJECT;
        }

        return type;
    }

    public Object toDocument(DBObject bson) {

        if (bson == null)
            return null;

        String type = (String) bson.get(TYPE);

        if (isDocument(type))
            return toSpaceDocument(bson);

        return toPojo(bson);
    }

    private Object toPojo(DBObject bson) {

        String className = (String) bson.get(TYPE);
        try {
            Class<?> type = getClassFor(className);
            Object pojo = repository.getConstructor(getClassFor(className))
                    .newInstance();

            for (String property : bson.keySet()) {

                if (TYPE.equals(property))
                    continue;

                Object value = bson.get(property);

                if (value == null)
                    continue;

                if (_ID.equals(property))
                    property = spaceTypeDescriptor.getIdPropertyName();

                ISetterMethod<Object> setter = repository.getSetter(type,
                        property);

                Object val = fromDBObject(value);

                setter.set(pojo, val);
            }
            return pojo;
        } catch (InvocationTargetException e) {
            throw new SpaceMongoException(
                    "can not invoke constructor or method: " + bson, e);
        } catch (InstantiationException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for: " + bson, e);
        } catch (IllegalAccessException e) {
            throw new SpaceMongoException(
                    "can not access constructor or method: " + bson, e);
        }

    }

    private Object toSpaceDocument(DBObject bson) {

        SpaceDocument document = new SpaceDocument((String) bson.get(TYPE));

        for (String property : bson.keySet()) {

            if (TYPE.equals(property))
                continue;

            Object value = bson.get(property);

            if (value == null)
                continue;

            if (_ID.equals(property))
                property = spaceTypeDescriptor.getIdPropertyName();

            document.setProperty(property, fromDBObject(value));
        }

        return document;
    }

    private boolean isDocument(String className) {
        try {
            Class.forName(className);
            return false;
        } catch (ClassNotFoundException e) {
        }

        return true;
    }

    public Object fromDBObject(Object value) {

        if (value == null)
            return null;

        switch (bsonType(value)) {
            case TYPE_OBJECTID:
                return null;
            case TYPE_ARRAY:
                return toExactArray((BasicDBList) value);
            case TYPE_OBJECT:
                return toExactObject(value);
            default:
                return value;
        }
    }

    @SuppressWarnings("unchecked")
    private Object toExactObject(Object value) {
        DBObject bson = (DBObject) value;

        if (bson.containsField(TYPE) && bson.containsField(VALUE)) {
            try {
                @SuppressWarnings("rawtypes")
                Class type = Class.forName((String) bson.get(TYPE));

                if (type.isEnum())
                    return Enum.valueOf(type, (String) bson.get(VALUE));
                else
                    return fromSpetialType((DBObject) value);

            } catch (ClassNotFoundException e) {
            }
        }

        return toDocument(bson);
    }

    private Object toExactArray(BasicDBList value) {

        if (value.size() < 1)
            throw new IllegalStateException("Illegal BSON array size: "
                    + value.size() + ", size must be at lest 1");

        Class<?> type = getClassFor((String) value.get(0));

        if (type.isArray()) {
            return toArray(type, value);
        } else if (Collection.class.isAssignableFrom(type)) {
            return toCollection(type, value);
        } else if (Map.class.isAssignableFrom(type)) {
            return toMap(type, value);
        }

        throw new SpaceMongoException("invalid Array/Collection/Map type: "
                + type.getName());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Map toMap(Class<?> type, BasicDBList value) {

        Map map = null;

        try {

            map = (Map) repository.getConstructor(type).newInstance();

            for (int i = 1; i < value.size(); i += 2) {
                Object key = fromDBObject(value.get(i));
                Object val = fromDBObject(value.get(i + 1));

                map.put(key, val);
            }

            return map;

        } catch (InvocationTargetException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        } catch (InstantiationException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        } catch (IllegalAccessException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        }
    }

    private Object[] toArray(Class<?> type, BasicDBList value) {

        Object[] array = (Object[]) Array.newInstance(type.getComponentType(),
                value.size() - 1);

        for (int i = 1; i < value.size(); i++) {
            Object v = fromDBObject(value.get(i));

            if (SpaceDocument.class.isAssignableFrom(type.getComponentType()))
                v = MongoDocumentObjectConverter.instance().toDocumentIfNeeded(
                        v, SpaceDocumentSupport.CONVERT);

            array[i - 1] = v;
        }

        return array;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Collection toCollection(Class<?> type, BasicDBList value) {

        Collection collection = null;

        try {
            collection = (Collection) repository.getConstructor(type)
                    .newInstance();

            for (int i = 1; i < value.size(); i++) {
                collection.add(fromDBObject(value.get(i)));
            }

        } catch (InvocationTargetException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        } catch (InstantiationException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        } catch (IllegalAccessException e) {
            throw new SpaceMongoException(
                    "Could not find default constructor for type: "
                            + type.getName(), e);
        }

        return collection;
    }

    public Class<?> getClassFor(String type) {
        try {
            return Class.forName(type);
        } catch (ClassNotFoundException e) {
            throw new SpaceMongoException("Could not resolve type for type: "
                    + type, e);
        }
    }

    public DBObject toDBObject(Object document) {

        if (document == null)
            return null;

        if (document instanceof SpaceDocument)
            return toDBObjectDocument((SpaceDocument) document);

        return toDBObjectPojo(document);
    }

    private DBObject toDBObjectDocument(SpaceDocument document) {
        DBObject bson = new BasicDBObject();


        Set<String> keys = document.getProperties().keySet();

        bson.put(TYPE, document.getTypeName());

        for (String property : keys) {

            Object value = document.getProperty(property);

            if (value == null)
                continue;

            if (spaceTypeDescriptor.getIdPropertyName().equals(property))
                property = _ID;

            bson.put(property, toObject(value));
        }

        return bson;

    }

    private DBObject toDBObjectPojo(Object pojo) {

        DBObject bson = new BasicDBObject();

        Map<String, Method> getters = repository.getGetters(pojo.getClass());

        Class<?> type = pojo.getClass();

        bson.put(TYPE, type.getName());

        for (String property : getters.keySet()) {
            Object value = null;
            try {

                value = repository.getGetter(type, property).get(pojo);

                if (value == null)
                    continue;

                if (spaceTypeDescriptor.getIdPropertyName().equals(property))
                    property = _ID;

                bson.put(property, toObject(value));

            } catch (IllegalArgumentException e) {
                throw new SpaceMongoException("Argument is: " + value, e);
            } catch (IllegalAccessException e) {
                throw new SpaceMongoException("Can not access method", e);
            } catch (InvocationTargetException e) {
                throw new SpaceMongoException("Can not invoke method", e);
            }
        }

        return bson;
    }

    public Object toObject(Object property) {

        switch (type(property)) {

            case TYPE_CHAR:
            case TYPE_FLOAT:
            case TYPE_BYTE:
            case TYPE_BIGDECIMAL:
            case TYPE_BIGINT:
                return toSpectialType(property);
            case TYPE_OBJECT:
                SpaceDocument document = MongoDocumentObjectConverter.instance()
                        .toSpaceDocument(property);

                return toDBObject(document);
            case TYPE_ENUM:
                return toEnum(property);
            case TYPE_ARRAY:
                return toArray(property);
            case TYPE_COLLECTION:
                return toCollection(property);
            case TYPE_MAP:
                return toMap(property);
            default:
                return property;
        }
    }

    private Object toEnum(Object property) {

        return new BasicDBObject(TYPE, property.getClass().getName()).append(
                VALUE, property.toString());
    }

    private BasicDBList toMap(Object property) {

        BasicDBList list = new BasicDBList();

        @SuppressWarnings("rawtypes")
        Map<?, ?> map = (Map) property;
        int index = 0;
        list.add(index++, property.getClass().getName());

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            list.add(index++, toObject(entry.getKey()));
            list.add(index++, toObject(entry.getValue()));
        }

        return list;
    }

    private BasicDBList toCollection(Object property) {
        BasicDBList list = new BasicDBList();

        @SuppressWarnings("rawtypes")
        Collection collection = (Collection) property;

        int index = 0;
        list.add(index++, property.getClass().getName());

        for (Object e : collection) {
            list.add(index++, toObject(e));
        }

        return list;
    }

    private BasicDBList toArray(Object property) {
        BasicDBList list = new BasicDBList();

        Object[] array = (Object[]) property;

        list.add(0, property.getClass().getName());

        for (int i = 0; i < array.length; i++) {

            list.add(i + 1, toObject(array[i]));
        }

        return list;
    }

    private Character toCharacter(Object value) {
        if (value == null)
            return null;

        if (value instanceof String)
            return ((String) value).charAt(0);

        throw new IllegalArgumentException("invalid value for Character: "
                + value);
    }

    private Object fromSpetialType(DBObject value) {
        String type = (String) value.get(TYPE);
        String val = (String) value.get(VALUE);

        if (BigInteger.class.getName().equals(type))
            return new BigInteger(val);
        else if (BigDecimal.class.getName().equals(type))
            return new BigDecimal(val);
        else if (Byte.class.getName().equals(type))
            return Byte.valueOf(val);
        else if (Float.class.getName().equals(type))
            return Float.valueOf(val);
        else if (Character.class.getName().equals(type))
            return toCharacter(val);

        throw new IllegalArgumentException("unkown value: " + value);
    }

    private DBObject toSpectialType(Object property) {

        return new BasicDBObject(TYPE, property.getClass().getName()).append(
                VALUE, property.toString());
    }
}
