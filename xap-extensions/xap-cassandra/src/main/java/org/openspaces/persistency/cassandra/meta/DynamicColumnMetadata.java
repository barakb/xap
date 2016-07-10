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

package org.openspaces.persistency.cassandra.meta;

import org.openspaces.persistency.cassandra.meta.mapping.node.TypeNode;

import me.prettyprint.hector.api.Serializer;

/**
 * A {@link TypeNode} implementation representing a dynamic column for which no metadata exists on
 * the matching Cassandra column family.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class DynamicColumnMetadata extends AbstractColumnMetadata {

    private final String fullName;
    private final Serializer<Object> dynamicPropertyValueSerializer;

    public DynamicColumnMetadata(String parentFullName,
                                 String name,
                                 Serializer<Object> dynamicPropertyValueSerializer) {
        this.dynamicPropertyValueSerializer = dynamicPropertyValueSerializer;
        fullName = (parentFullName != null ? parentFullName + "." : "") + name;
    }

    public DynamicColumnMetadata(
            String fullName,
            Serializer<Object> dynamicPropertyValueSerializer) {
        this.fullName = fullName;
        this.dynamicPropertyValueSerializer = dynamicPropertyValueSerializer;
    }

    @Override
    public String getFullName() {
        return fullName;
    }

    @Override
    public Serializer<Object> getSerializer() {
        return dynamicPropertyValueSerializer;
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException("Not implemented in dynamic columns");
    }

    @Override
    public Class<?> getType() {
        throw new UnsupportedOperationException("Not implemented in dynamic columns");
    }

}
