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

import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;

/**
 * An interface to describe a property's metadata that is part of a type hierarcy.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public interface TypeNode {

    /**
     * @return The type node full name including that path to this this type nodes matching
     * property.
     */
    String getFullName();

    /**
     * @return The type node simple property name (without the path).
     */
    String getName();

    /**
     * @return The type of this type nodes matching property.
     */
    Class<?> getType();

    /**
     * Recursively write the value matching this type node and all its children to a {@link
     * ColumnFamilyRow} instance.
     *
     * @param value   The value matching this type node.
     * @param row     The row to write the value into.
     * @param context the current {@link TypeNodeContext}.
     */
    void writeToColumnFamilyRow(Object value, ColumnFamilyRow row, TypeNodeContext context);

    /**
     * Recursively read the value matching this type node property from the {@link
     * ColumnFamilyRow}.
     *
     * @param row     The row to read from.
     * @param context The current {@link TypeNodeContext}.
     * @return The value read from the {@link ColumnFamilyRow}.
     */
    Object readFromColumnFamilyRow(ColumnFamilyRow row, TypeNodeContext context);
}
