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

import org.openspaces.persistency.cassandra.meta.mapping.filter.PropertyContext;

/**
 * Context used during type introspection and during read/write operations
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class TypeNodeContext implements PropertyContext {

    private final TypeNodeIntrospector typeNodeIntrospector;
    private final boolean useDynamicPropertySerializerForDynamicColumns;

    private boolean isDynamic;
    private int currentNestingLevel = 0;

    private String currentPropertyPath = null;
    private String currentPropertyName = null;
    private Class<?> currentPropertyType = null;

    public TypeNodeContext() {
        this(null, true);
    }

    public TypeNodeContext(
            TypeNodeIntrospector typeNodeIntrospector,
            boolean useDynamicPropertySerializerForDynamicColumns) {
        this.typeNodeIntrospector = typeNodeIntrospector;
        this.useDynamicPropertySerializerForDynamicColumns = useDynamicPropertySerializerForDynamicColumns;

    }

    /**
     * @return Current nesting level (used during type introspection and during write operations).
     */
    @Override
    public int getCurrentNestingLevel() {
        return currentNestingLevel;
    }

    /**
     * Increase the current nesting level.
     */
    public void increaseNestingLevel() {
        currentNestingLevel++;
    }

    /**
     * Decrease the current nesting level.
     */
    public void descreaseNestingLevel() {
        currentNestingLevel--;
    }

    /**
     * @return Whether the current location in the type introspaction hierarcy is a descendent of a
     * dynamic type node property.
     */
    @Override
    public boolean isDynamic() {
        return isDynamic;
    }

    /**
     * Sets the current dynamic context value.
     */
    public void setDynamic(boolean isDynamic) {
        this.isDynamic = isDynamic;
    }

    /**
     * @return The type node introspector.
     */
    public TypeNodeIntrospector getTypeNodeIntrospector() {
        return typeNodeIntrospector;
    }

    /**
     * @return <code>true</code> if dynamic columns should be serialized using the dynamic property
     * serializer.
     */
    public boolean isUseDynamicPropertySerializerForDynamicColumns() {
        return useDynamicPropertySerializerForDynamicColumns;
    }

    /**
     * Sets the current property context information (used in {@link org.openspaces.persistency.cassandra.meta.mapping.filter.FlattenedPropertiesFilter})
     */
    public void setCurrentPropertyContext(String propertyPath, String propertyName, Class<?> propertyType) {
        this.currentPropertyPath = propertyPath;
        ;
        this.currentPropertyName = propertyName;
        this.currentPropertyType = propertyType;
    }

    /* (non-Javadoc)
     * @see org.openspaces.persistency.cassandra.meta.mapping.filter.PropertyContext#getPath()
     */
    @Override
    public String getPath() {
        return currentPropertyPath;
    }

    /* (non-Javadoc)
     * @see org.openspaces.persistency.cassandra.meta.mapping.filter.PropertyContext#getName()
     */
    @Override
    public String getName() {
        return currentPropertyName;
    }

    /* (non-Javadoc)
     * @see org.openspaces.persistency.cassandra.meta.mapping.filter.PropertyContext#getType()
     */
    @Override
    public Class<?> getType() {
        return currentPropertyType;
    }
}
