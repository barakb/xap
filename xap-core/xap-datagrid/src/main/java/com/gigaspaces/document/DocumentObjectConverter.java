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

package com.gigaspaces.document;

import com.gigaspaces.internal.metadata.SpaceDocumentSupportHelper;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpaceMetadataException;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

/**
 * This class provides method to convert POJO classes to {@link SpaceDocument} and vice versa.
 *
 * @author Niv Ingberg
 * @since 8.0.1
 */
@com.gigaspaces.api.InternalApi
public class DocumentObjectConverter {
    private static final DocumentObjectConverter _instance = new DocumentObjectConverter();

    /**
     * Returns a singleton instance of DocumentObjectConverter.
     */
    public static DocumentObjectConverter instance() {
        return _instance;
    }

    private final boolean _throwExceptionOnTypeMissing;

    /**
     * Default constructor.
     */
    public DocumentObjectConverter() {
        this(true);
    }

    protected DocumentObjectConverter(boolean throwExceptionOnTypeMissing) {
        _throwExceptionOnTypeMissing = throwExceptionOnTypeMissing;
    }

    /**
     * Converts the specified POJO to a {@link SpaceDocument}.
     *
     * @param object Object to convert.
     * @return A {@link SpaceDocument} whose type and properties are the same as the object.
     */
    public SpaceDocument toSpaceDocument(Object object) {
        // Get conversion context to handle cyclic reference.
        ToDocumentConvertionContext conversionContext = new ToDocumentConvertionContext();
        return toSpaceDocumentInternal(object, conversionContext);
    }

    private SpaceDocument toSpaceDocumentInternal(Object object, ToDocumentConvertionContext conversionContext) {
        // Check if conversion is required:
        if (object == null)
            return null;
        if (object instanceof SpaceDocument)
            return (SpaceDocument) object;

        // Get space type info of object's class:
        final SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(object.getClass());
        // Create document:
        final SpaceDocument document = new SpaceDocument(typeInfo.getName());
        // Copy fixed properties if any:
        if (typeInfo.getNumOfSpaceProperties() != 0) {
            // Get object properties values:
            Object[] values = typeInfo.getSpacePropertiesValues(object, true);
            // Copy values to properties:
            for (int i = 0; i < values.length; i++) {
                SpacePropertyInfo property = typeInfo.getProperty(i);
                // Convert property value to document if needed:
                Object value = toDocumentIfNeededInternal(values[i], property.getDocumentSupport(), conversionContext);
                // Set converted property in result document:
                document.setProperty(property.getName(), value);
            }
        }
        // Copy dynamic properties (if any):
        if (typeInfo.getDynamicPropertiesProperty() != null)
            document.addProperties((Map<String, Object>) typeInfo.getDynamicPropertiesProperty().getValue(object));

        // If pojo has a version property, copy its value:
        if (typeInfo.getVersionProperty() != null)
            document.setVersion((Integer) typeInfo.getVersionProperty().getValue(object));
        // If pojo has a persist property, copy its value:
        if (typeInfo.getPersistProperty() != null)
            document.setTransient(!(Boolean) typeInfo.getPersistProperty().getValue(object));

        // Return result:
        return document;
    }

    /**
     * Converts the specified space document to a POJO.
     *
     * @param document Document to convert.
     * @return A POJO whose class and properties are the same as the document.
     */
    public Object toObject(SpaceDocument document) {
        // Get conversion context to handle cyclic reference.
        FromDocumentConvertionContext conversionContext = new FromDocumentConvertionContext();
        return toObjectInternal(document, conversionContext);
    }

    private Object toObjectInternal(SpaceDocument document, FromDocumentConvertionContext conversionContext) {
        // Check if conversion is required:
        if (document == null)
            return null;

        // Get space type info of document's type:
        final SpaceTypeInfo typeInfo;
        if (_throwExceptionOnTypeMissing) {
            typeInfo = SpaceTypeInfoRepository.getGlobalRepository().getByName(document.getTypeName());
        } else {
            typeInfo = SpaceTypeInfoRepository.getGlobalRepository().getByNameIfExists(document.getTypeName());
            if (typeInfo == null)
                return document;
        }

        // Init fixed properties values array:
        int numOfSpaceProperties = typeInfo.getNumOfSpaceProperties();
        final Object[] fixedProperties = numOfSpaceProperties != 0 ? new Object[numOfSpaceProperties] : null;
        // Init dynamic properties values map:
        Map<String, Object> dynamicProperties = null;
        // Iterate document's properties, copy to pojo's fixed/dynamic properties containers as needed:
        for (Map.Entry<String, Object> entry : document.getProperties().entrySet()) {
            final String propertyName = entry.getKey();
            final Object propertyValue = entry.getValue();
            // Get space property (if exists):
            SpacePropertyInfo pojoProperty = typeInfo.getProperty(propertyName);
            // Check if property is fixed/dynamic and copy as needed:
            int position = typeInfo.indexOf(pojoProperty);
            if (position != -1)
                fixedProperties[position] = fromDocumentIfNeededInternal(propertyValue, pojoProperty.getDocumentSupport()
                        , pojoProperty.getType(), conversionContext);
            else {
                if (dynamicProperties == null)
                    dynamicProperties = new DocumentProperties();
                dynamicProperties.put(propertyName, propertyValue);
            }
        }

        // common validation
        if (dynamicProperties != null && typeInfo.getDynamicPropertiesProperty() == null) {
            throw new SpaceMetadataException("Cannot set dynamic properties in type '"
                    + typeInfo.getName()
                    + "' because it does not support dynamic properties.");
        }

        // Create object:
        if (typeInfo.hasConstructorProperties()) {
            return typeInfo.createConstructorBasedInstance(fixedProperties,
                    dynamicProperties,
                    null /* uid */,
                    document.getVersion(),
                    Long.MAX_VALUE /* lease */,
                    !document.isTransient());
        } else {
            final Object object = typeInfo.createInstance();
            // Set fixed properties values, if any:
            if (fixedProperties != null)
                typeInfo.setSpacePropertiesValues(object, fixedProperties);
            // Set dynamic properties values, if any:
            if (dynamicProperties != null)
                typeInfo.getDynamicPropertiesProperty().setValue(object, dynamicProperties);

            // If pojo has a version property, copy its value:
            if (typeInfo.getVersionProperty() != null)
                typeInfo.getVersionProperty().setValue(object, document.getVersion());
            // If pojo has a persist property, copy its value:
            if (typeInfo.getPersistProperty() != null)
                typeInfo.getPersistProperty().setValue(object, !document.isTransient());

            // Return result:
            return object;
        }
    }

    public Object toDocumentIfNeeded(Object object, SpaceDocumentSupport documentSupport) {
        return toDocumentIfNeededInternal(object, documentSupport, null);
    }

    private Object toDocumentIfNeededInternal(Object object, SpaceDocumentSupport documentSupport
            , ToDocumentConvertionContext conversionContext) {
        if (object == null)
            return null;
        if (object instanceof List && object instanceof RandomAccess)
            return toDocumentListIfNeeded((List) object, documentSupport, conversionContext);
        if (object instanceof Collection)
            return toDocumentCollectionIfNeeded((Collection<?>) object, documentSupport, conversionContext);
        if (object instanceof Object[])
            return toDocumentArrayIfNeeded((Object[]) object, documentSupport, conversionContext);
        if (object instanceof Map)
            return toDocumentMapIfNeeded((Map<Object, Object>) object, documentSupport, conversionContext);

        switch (documentSupport) {
            case COPY:
                return object;
            case CONVERT:
                return convertObjectToDocument(object, conversionContext);
            case DEFAULT:
                documentSupport = SpaceDocumentSupportHelper.getDefaultDocumentSupport(object.getClass());
                if (documentSupport == SpaceDocumentSupport.CONVERT)
                    return convertObjectToDocument(object, conversionContext);
                return object;
            default:
                throw new IllegalArgumentException("Unsupported SpaceDocumentSupport - " + documentSupport);
        }
    }

    private SpaceDocument convertObjectToDocument(Object object, ToDocumentConvertionContext conversionContext) {
        if (conversionContext == null)
            return toSpaceDocument(object);

        // get the object from the context if exist.
        SpaceDocument document = conversionContext.getConvertedDocument(object);
        if (document != null)
            return document;

        // The object does not exist in the context- initialize context state.
        conversionContext.initConvertionState(object);

        // convert the object to SpaceDocument and update the context with the converted document.
        document = toSpaceDocumentInternal(object, conversionContext);
        conversionContext.setConvertedDocument(object, document);

        return conversionContext.getConvertedDocument(object);
    }

    protected Object fromDocumentIfNeeded(Object object, SpaceDocumentSupport documentSupport, Class<?> expectedType) {
        return fromDocumentIfNeededInternal(object, documentSupport, expectedType, null);
    }

    private Object fromDocumentIfNeededInternal(Object object, SpaceDocumentSupport documentSupport, Class<?> expectedType
            , FromDocumentConvertionContext conversionContext) {
        if (object == null)
            return null;
        if (object instanceof List && object instanceof RandomAccess)
            return fromDocumentListIfNeeded((List<Object>) object, documentSupport, expectedType, conversionContext);
        if (object instanceof Collection)
            return fromDocumentCollectionIfNeeded((Collection<?>) object, documentSupport, expectedType, conversionContext);
        if (object instanceof Object[])
            return fromDocumentArrayIfNeeded((Object[]) object, documentSupport, expectedType, conversionContext);
        if (object instanceof Map)
            return fromDocumentMapIfNeeded((Map<Object, Object>) object, documentSupport, conversionContext);

        switch (documentSupport) {
            case COPY:
                return object;
            case CONVERT:
                if (object instanceof SpaceDocument)
                    return convertDocumentToObject((SpaceDocument) object, expectedType, conversionContext);
                if (object.getClass().equals(expectedType) || expectedType.isInstance(object))
                    return object;
                throw new IllegalArgumentException("Cannot convert from document to '" + expectedType.getName() + "' - type '" + object.getClass() + "' is not a document.");
            case DEFAULT:
                if (object instanceof SpaceDocument)
                    return convertDocumentToObject((SpaceDocument) object, expectedType, conversionContext);
                return object;
            default:
                throw new IllegalArgumentException("Unsupported SpaceDocumentSupport - " + documentSupport);
        }
    }

    private Object convertDocumentToObject(SpaceDocument document, Class<?> expectedType, FromDocumentConvertionContext conversionContext) {
        if (conversionContext == null)
            return toObject(document);

        // If the document exists in the context, get the converted Object from it.
        Object object = conversionContext.getConvertedObject(document);
        if (object != null)
            return object;

        // The document does not exist in the context- initialize context state.
        conversionContext.initConvertionState(document);

        // convert the SpaceDocument to Object and update the context.
        object = toObjectInternal(document, conversionContext);
        conversionContext.setConvertedObject(document, object);

        return conversionContext.getConvertedObject(document);
    }

    private Object toDocumentMapIfNeeded(Map<Object, Object> map, SpaceDocumentSupport documentSupport, ToDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return map;

        Map<Object, Object> result = null;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            Object convertedKey = toDocumentIfNeededInternal(key, documentSupport, conversionContext);
            Object convertedValue = toDocumentIfNeededInternal(value, documentSupport, conversionContext);
            if (key != convertedKey) {
                if (result == null)
                    result = CollectionUtils.cloneMap(map);
                result.remove(key);
                result.put(convertedKey, convertedValue);
            } else if (value != convertedValue) {
                if (result == null)
                    result = CollectionUtils.cloneMap(map);
                result.put(key, convertedValue);
            }
        }

        return result != null ? result : map;
    }

    private Object fromDocumentMapIfNeeded(Map<Object, Object> map, SpaceDocumentSupport documentSupport, FromDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return map;

        Map<Object, Object> result = null;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            Object convertedKey = fromDocumentIfNeededInternal(key, documentSupport, Object.class, conversionContext);
            Object convertedValue = fromDocumentIfNeededInternal(value, documentSupport, Object.class, conversionContext);
            if (key != convertedKey) {
                if (result == null)
                    result = CollectionUtils.cloneMap(map);
                result.remove(key);
                result.put(convertedKey, convertedValue);
            } else if (value != convertedValue) {
                if (result == null)
                    result = CollectionUtils.cloneMap(map);
                result.put(key, convertedValue);
            }
        }
        return result != null ? result : map;
    }

    private Object toDocumentListIfNeeded(List<Object> list, SpaceDocumentSupport documentSupport, ToDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return list;

        List<Object> result = null;
        final int length = list.size();
        for (int i = 0; i < length; i++) {
            Object item = list.get(i);
            Object convertedItem = toDocumentIfNeededInternal(item, documentSupport, conversionContext);
            if (item != convertedItem) {
                if (result == null)
                    result = CollectionUtils.cloneList(list);
                result.set(i, convertedItem);
            }
        }

        return result != null ? result : list;
    }

    private Object fromDocumentListIfNeeded(List<Object> list, SpaceDocumentSupport documentSupport, Class<?> expectedType
            , FromDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return list;

        List<Object> result = null;
        final int length = list.size();
        for (int i = 0; i < length; i++) {
            Object item = list.get(i);
            Object convertedItem = fromDocumentIfNeededInternal(item, documentSupport, Object.class, conversionContext);
            if (item != convertedItem) {
                if (result == null)
                    result = CollectionUtils.cloneList(list);
                result.set(i, convertedItem);
            }
        }

        return result != null ? result : list;
    }

    private Object toDocumentCollectionIfNeeded(Collection<?> collection, SpaceDocumentSupport documentSupport, ToDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return collection;
        // Create a new collection of the same type to host the converted values:
        SpaceTypeInfo collectionInfo = SpaceTypeInfoRepository.getTypeInfo(collection.getClass());
        Collection<Object> result = (Collection<Object>) collectionInfo.createInstance();
        // Convert each item from the source collection as needed and add it to the result collection:
        for (Object item : collection)
            result.add(toDocumentIfNeededInternal(item, documentSupport, conversionContext));

        return result;
    }

    private Object fromDocumentCollectionIfNeeded(Collection<?> collection, SpaceDocumentSupport documentSupport, Class<?> expectedType
            , FromDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return collection;
        // Create a new collection of the same type to host the converted values:
        SpaceTypeInfo collectionInfo = SpaceTypeInfoRepository.getTypeInfo(collection.getClass());
        Collection<Object> result = (Collection<Object>) collectionInfo.createInstance();
        // Convert each item from the source collection as needed and add it to the result collection:
        for (Object item : collection)
            result.add(fromDocumentIfNeededInternal(item, documentSupport, Object.class, conversionContext));

        return result;
    }

    private Object toDocumentArrayIfNeeded(Object[] array, SpaceDocumentSupport documentSupport, ToDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return array;
        int arrayLength = array.length;
        // Create a temp object array list of the same length to host the converted values:
        Object[] tempArray = new Object[arrayLength];
        // Convert each item from the source array as needed and put it to the temp list:
        int documentCount = 0;
        for (int i = 0; i < arrayLength; i++) {
            tempArray[i] = toDocumentIfNeededInternal(array[i], documentSupport, conversionContext);
            if (tempArray[i] instanceof SpaceDocument)
                documentCount++;
        }
        // Create an array of the wanted type to host the converted values:
        Object result = null;
        if (documentCount == 0)
            result = Array.newInstance(array.getClass().getComponentType(), arrayLength);
        else if (documentCount == arrayLength)
            result = new SpaceDocument[arrayLength];
        else
            result = new Object[arrayLength];
        // copy the temp list to the result array
        System.arraycopy(tempArray, 0, result, 0, arrayLength);

        return result;
    }

    private Object fromDocumentArrayIfNeeded(Object[] array, SpaceDocumentSupport documentSupport, Class<?> expectedType
            , FromDocumentConvertionContext conversionContext) {
        // If no conversion is required, return:
        if (documentSupport == SpaceDocumentSupport.COPY)
            return array;
        // Create an array of the same type and length to host the converted values:
        Class<?> componentType = expectedType.isArray() ? expectedType.getComponentType() : Object.class;
        Object[] result = (Object[]) Array.newInstance(componentType, array.length);
        // Convert each item from the source array as needed and put it to the result array:
        for (int i = 0; i < array.length; i++)
            result[i] = fromDocumentIfNeededInternal(array[i], documentSupport, componentType, conversionContext);

        return result;
    }

    private static class ToDocumentConvertionContext {
        private IdentityHashMap<Object, SpaceDocumentHolder> convertedObjectsMap;

        public ToDocumentConvertionContext() {
            convertedObjectsMap = null;
        }

        public void setConvertedDocument(Object object, SpaceDocument document) {
            if (convertedObjectsMap.get(object) != null)
                convertedObjectsMap.get(object).setDocument(document);
        }

        public void initConvertionState(Object object) {
            if (convertedObjectsMap == null)
                convertedObjectsMap = new IdentityHashMap<Object, SpaceDocumentHolder>();
            convertedObjectsMap.put(object, new SpaceDocumentHolder());
        }

        public SpaceDocument getConvertedDocument(Object object) {
            if (convertedObjectsMap != null && convertedObjectsMap.containsKey(object))
                return convertedObjectsMap.get(object).getSpaceDocument();
            return null;
        }
    }

    private static class FromDocumentConvertionContext {
        private IdentityHashMap<SpaceDocument, ObjectHolder> convertedDocumentsMap;

        public FromDocumentConvertionContext() {
            convertedDocumentsMap = null;
        }

        public void setConvertedObject(SpaceDocument document, Object object) {
            if (convertedDocumentsMap.get(document) != null)
                convertedDocumentsMap.get(document).setObject(object);
        }

        public void initConvertionState(SpaceDocument document) {
            if (convertedDocumentsMap == null)
                convertedDocumentsMap = new IdentityHashMap<SpaceDocument, ObjectHolder>();
            convertedDocumentsMap.put(document, new ObjectHolder());
        }

        public Object getConvertedObject(SpaceDocument document) {
            if (convertedDocumentsMap != null && convertedDocumentsMap.containsKey(document))
                return convertedDocumentsMap.get(document).getObject();
            return null;
        }
    }

    private static class SpaceDocumentHolder {
        SpaceDocument document = new SpaceDocument();

        public SpaceDocument getSpaceDocument() {
            return document;
        }

        public void setDocument(SpaceDocument otherDocument) {
            this.document.initFromDocument(otherDocument);
        }

    }

    private static class ObjectHolder {
        Object object;

        public Object getObject() {
            return object;
        }

        public void setObject(Object object) {
            this.object = object;
        }
    }
}
