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

package com.gigaspaces.entry;

import java.util.Map;

/**
 * @author Niv Ingberg
 * @Since 8.0
 */
public interface VirtualEntry {
    /**
     * @return the type name.
     */
    String getTypeName();

    /**
     * Sets the document type name.
     *
     * @param typeName Type name to set.
     * @return This document (used for fluent method chaining).
     */
    VirtualEntry setTypeName(String typeName);

    /**
     * Returns the entry's version.
     *
     * @return This document (used for fluent method chaining).
     */
    int getVersion();

    /**
     * Sets the entry's version.
     *
     * @return This document (used for fluent method chaining).
     */
    VirtualEntry setVersion(int version);

    /**
     * @return true if the entry is transient, false otherwise.
     */
    boolean isTransient();

    /**
     * Sets whether or not the entry is transient.
     *
     * @return This document (used for fluent method chaining).
     */
    VirtualEntry setTransient(boolean isTransient);

    /**
     * Returns <tt>true</tt> if this document contains the specified property.
     *
     * @param name Name of property to look for.
     * @return Returns true if this document contains the specified property, false otherwise.
     */
    boolean containsProperty(String name);

    /**
     * Gets a property's value by its name. If the property does not exist in this document null is
     * returned.
     *
     * @param name Name of property.
     * @return Value of property.
     */
    <T> T getProperty(String name);

    /**
     * Sets a property's value by its name. If the property is already defined in this document the
     * existing value is overridden.
     *
     * @param name  Name of property
     * @param value New value of property.
     * @return This document (used for fluent method chaining).
     */
    VirtualEntry setProperty(String name, Object value);

    /**
     * Removes the property from the document (if it exists).
     *
     * @param name Name of property to remove.
     * @return Previous value associated with specified property, or <tt>null</tt> if there was no
     * such property.
     */
    <T> T removeProperty(String name);

    /**
     * Returns an unmodifiable view of the document's properties. Attempts to modify the returned
     * map, whether direct or via its collection views, result in an <tt>UnsupportedOperationException</tt>.<p>
     *
     * @return Reference to document's properties map.
     */
    Map<String, Object> getProperties();

    /**
     * Adds the specified properties to the document's current properties.
     *
     * @param properties Properties to add.
     * @return This document (used for fluent method chaining).
     */
    VirtualEntry addProperties(Map<String, Object> properties);
}
