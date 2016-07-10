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

package org.openspaces.persistency.cassandra.meta.mapping.node;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.pojo.PojoPropertyInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfoRepository;

import org.openspaces.persistency.cassandra.CassandraPersistencyConstants;
import org.openspaces.persistency.cassandra.error.SpaceCassandraTypeIntrospectionException;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.data.ColumnData;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;
import org.openspaces.persistency.cassandra.meta.types.PrimitiveClassUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link SpaceDocument} based implementation of {@link TopLevelTypeNode}.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class SpaceDocumentTopLevelTypeNode extends SpaceDocumentTypeNode
        implements TopLevelTypeNode {

    private static final long serialVersionUID = 1L;
    public static final byte SERIAL_VER = Byte.MIN_VALUE;

    private Class<?> keyType;

    /* for Externalizable */
    public SpaceDocumentTopLevelTypeNode() {

    }

    public SpaceDocumentTopLevelTypeNode(
            String typeName,
            String keyName,
            Class<?> keyType,
            Map<String, TypeNode> initialChildren,
            TypeNodeContext context) {
        super(typeName, null, keyName, initialChildren, context);
        this.keyType = keyType;
    }

    @Override
    public String getKeyName() {
        return getName();
    }

    @Override
    public Class<?> getKeyType() {
        return keyType;
    }

    @Override
    protected String generateFullName(String parentFullName, String name) {
        return null;
    }

    @Override
    protected boolean shouldSkipEntryWrite(String key, Object value) {
        return getKeyName().equals(key) ||
                super.shouldSkipEntryWrite(key, value);
    }

    @Override
    protected void writePropertyToColumnFamilyRow(ColumnFamilyRow row, String propertyName, Object propertyValue,
                                                  TypeNodeContext context) {

        propertyValue = context
                .getTypeNodeIntrospector()
                .convertFromSpaceDocumentIfNeeded(propertyValue, getChildren().get(propertyName));

        super.writePropertyToColumnFamilyRow(row, propertyName, propertyValue, context);
    }

    @Override
    public SpaceDocument readFromColumnFamilyRow(ColumnFamilyRow row, TypeNodeContext context) {
        ColumnFamilyMetadata metadata = row.getColumnFamilyMetadata();
        SpaceDocument result = super.readFromColumnFamilyRow(row, context);
        Object keyValue = row.getKeyValue();

        result.setProperty(metadata.getKeyName(), keyValue);

        Map<String, Object> properties = new HashMap<String, Object>(result.getProperties());
        handleDynamicColumns(row, properties, context);
        result.addProperties(properties);

        return result;
    }

    private void handleDynamicColumns(ColumnFamilyRow row, Map<String, Object> properties, TypeNodeContext context) {
        Map<String, Object> compoundProperties = new HashMap<String, Object>();
        compoundProperties.put("" /* root */, properties);

        // The very important assumption here is that columns are sorted by column name
        for (ColumnData dynamicColumn : row.getDynamicColumns()) {
            String columnName = dynamicColumn.getColumnMetadata().getFullName();

            int lastDotIndex = columnName.lastIndexOf('.');
            String propertyParentPath;
            String propertyName;
            if (lastDotIndex == -1) {
                propertyParentPath = "";
                propertyName = columnName;
            } else {
                propertyParentPath = columnName.substring(0, lastDotIndex);
                propertyName = columnName.substring(lastDotIndex + 1);
            }

            Object value = dynamicColumn.getValue();
            value = createCompoundColumnIfNecessary(compoundProperties, columnName, value, context);

            Object propertyParent = getPropertyParent(compoundProperties, propertyParentPath, context);

            addDynamicProperty(propertyName, value, propertyParent, context);
        }
    }

    private Object createCompoundColumnIfNecessary(
            Map<String, Object> compoundProperties,
            String columnName,
            Object value,
            TypeNodeContext context) {
        if (value instanceof String) {
            String strValue = (String) value;
            if (strValue.startsWith(CassandraPersistencyConstants.SPACE_DOCUMENT_COLUMN_PREFIX)) {
                String typeName = strValue.substring(CassandraPersistencyConstants.SPACE_DOCUMENT_COLUMN_PREFIX.length());
                value = createDocumentFromTypeName(compoundProperties, columnName, typeName);
            } else if (strValue.equals(CassandraPersistencyConstants.MAP_ENTRY_COLUMN)) {
                Map<String, Object> map = new HashMap<String, Object>();
                compoundProperties.put(columnName, map);
                value = map;
            } else if (strValue.startsWith(CassandraPersistencyConstants.POJO_ENTRY_COLUMN_PREFIX)) {
                String typeName = strValue.substring(CassandraPersistencyConstants.POJO_ENTRY_COLUMN_PREFIX.length());
                try {
                    value = createPojoFromTypeName(compoundProperties, columnName, typeName, context);
                } catch (Exception e) {
                    throw new SpaceCassandraTypeIntrospectionException("Could not create new instance for type name: " +
                            typeName, e);
                }
            }
        }
        return value;
    }

    private SpaceDocument createDocumentFromTypeName(
            Map<String, Object> compoundProperties,
            String columnName,
            String typeName) {
        SpaceDocument document = new SpaceDocument(typeName);
        compoundProperties.put(columnName, document);
        return document;
    }

    @SuppressWarnings("unchecked")
    private Object createPojoFromTypeName(
            Map<String, Object> compoundProperties,
            String columnName,
            String typeName,
            TypeNodeContext context) throws Exception {

        Class<?> type = Class.forName(typeName);
        Constructor<Object> constructor = (Constructor<Object>) type.getConstructor();
        Object object = context.getTypeNodeIntrospector()
                .getProcedureCache()
                .constructorFor(constructor)
                .newInstance();
        compoundProperties.put(columnName, object);
        return object;
    }

    @SuppressWarnings("rawtypes")
    private Object getPropertyParent(
            Map<String, Object> compoundProperties,
            String propertyParentPath,
            TypeNodeContext context) {
        Object propertyParent = compoundProperties.get(propertyParentPath);
        if (propertyParent == null) {
            StringBuilder currentPath = new StringBuilder();
            Object currentParent = compoundProperties.get(currentPath.toString()); // root

            // column name is compound because root parent always exists
            String[] tokens = propertyParentPath.split("\\.");
            for (String token : tokens) {
                if (currentPath.length() > 0) {
                    currentPath.append(".");
                }
                currentPath.append(token);

                Object newParent = compoundProperties.get(token);
                if (newParent == null) {
                    if (currentParent instanceof SpaceDocument) {
                        currentParent = ((SpaceDocument) currentParent).getProperty(token);
                    } else if (currentParent instanceof Map) {
                        currentParent = ((Map) currentParent).get(token);
                    } else {
                        // assuming pojo
                        PojoTypeInfo pojoTypeInfo = PojoTypeInfoRepository.getPojoTypeInfo(currentParent.getClass());
                        PojoPropertyInfo propertyInfo = pojoTypeInfo.getProperty(token);
                        if (propertyInfo == null || propertyInfo.getGetterMethod() == null) {
                            throw new SpaceCassandraTypeIntrospectionException("Could not find getter method for " +
                                    "property " + token + " of type: " + currentParent.getClass(), null);
                        }

                        try {
                            currentParent = context.getTypeNodeIntrospector()
                                    .getProcedureCache()
                                    .getterMethodFor(propertyInfo.getGetterMethod())
                                    .get(currentParent);
                        } catch (IllegalArgumentException e) {
                            throw new SpaceCassandraTypeIntrospectionException("Failed getting value for " +
                                    "property " + token + " of parent: " + currentParent.getClass(), e);
                        } catch (IllegalAccessException e) {
                            throw new SpaceCassandraTypeIntrospectionException("Failed getting value for " +
                                    "property " + token + " of parent: " + currentParent.getClass(), e);
                        } catch (InvocationTargetException e) {
                            throw new SpaceCassandraTypeIntrospectionException("Failed getting value for " +
                                    "property " + token + " of parent: " + currentParent.getClass(), e);
                        }
                    }

                    if (currentParent == null) {
                        throw new SpaceCassandraTypeIntrospectionException("Could not find value parent node for " +
                                "path: " + currentPath.toString(), null);
                    }

                    compoundProperties.put(currentPath.toString(), currentParent);
                } else {
                    currentParent = newParent;
                }
            }

            propertyParent = currentParent;
        }

        return propertyParent;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void addDynamicProperty(
            String propertyName,
            Object value,
            Object propertyParent,
            TypeNodeContext context) {
        if (propertyParent instanceof SpaceDocument) {
            ((SpaceDocument) propertyParent).setProperty(propertyName, value);
        } else if (propertyParent instanceof Map) {
            ((Map) propertyParent).put(propertyName, value);
        } else {
            // assuming pojo for now
            PojoTypeInfo pojoTypeInfo = PojoTypeInfoRepository.getPojoTypeInfo(propertyParent.getClass());
            PojoPropertyInfo propertyInfo = pojoTypeInfo.getProperty(propertyName);
            if (propertyInfo == null || propertyInfo.getSetterMethod() == null) {
                throw new SpaceCassandraTypeIntrospectionException("Could not find setter method for " +
                        "property " + propertyName + " of parent: " + propertyParent.getClass(), null);
            }

            try {
                context.getTypeNodeIntrospector()
                        .getProcedureCache()
                        .setterMethodFor(propertyInfo.getSetterMethod())
                        .set(propertyParent, value);
            } catch (IllegalArgumentException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed setting value for " +
                        "property " + propertyName + " of parent: " + propertyParent.getClass(), e);
            } catch (IllegalAccessException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed setting value for " +
                        "property " + propertyName + " of parent: " + propertyParent.getClass(), e);
            } catch (InvocationTargetException e) {
                throw new SpaceCassandraTypeIntrospectionException("Failed setting value for " +
                        "property " + propertyName + " of parent: " + propertyParent.getClass(), e);
            }

        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(SERIAL_VER);
        IOUtils.writeString(out, name);
        IOUtils.writeString(out, fullName);
        IOUtils.writeString(out, typeName);
        IOUtils.writeString(out, keyType.getName());
        IOUtils.writeObject(out, getChildren());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        in.readByte(); // SERIAL_VER (currently, unused)
        name = IOUtils.readString(in);
        fullName = IOUtils.readString(in);
        typeName = IOUtils.readString(in);

        String keyTypeName = IOUtils.readString(in);
        if (PrimitiveClassUtils.isPrimitive(keyTypeName)) {
            keyType = PrimitiveClassUtils.getPrimitive(keyTypeName);
        } else {
            keyType = Class.forName(keyTypeName, false, Thread.currentThread().getContextClassLoader());
        }

        Map<String, TypeNode> children = IOUtils.readObject(in);
        getChildren().putAll(children);
    }
}
