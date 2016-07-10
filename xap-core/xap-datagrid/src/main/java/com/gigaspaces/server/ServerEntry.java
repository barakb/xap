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


package com.gigaspaces.server;

import com.gigaspaces.metadata.SpaceTypeDescriptor;

/**
 * Represents an entry stored in the space.
 *
 * @author Niv Ingberg.
 * @since 7.1
 */
public interface ServerEntry {
    /**
     * Gets the entry's type descriptor.
     *
     * @return Current entry's type descriptor.
     */
    SpaceTypeDescriptor getSpaceTypeDescriptor();

    /**
     * Gets the specified fixed property's value. The returned value is a reference to the actual
     * property kept in the space.
     *
     * @param position Position of requested property.
     * @return Requested property's value in current entry.
     */
    Object getFixedPropertyValue(int position);

    /**
     * Gets the specified property's value. The returned value is a reference to the actual property
     * kept in the space.
     *
     * @param name Name of requested property.
     * @return Requested property's value in current entry.
     */
    Object getPropertyValue(String name);

    /**
     * Gets the specified path's value. The returned value is a reference to the actual property
     * kept in the space.
     *
     * @param path Path pointing to the requested property.
     * @return Requested path's value in current entry.
     */
    Object getPathValue(String path);

    /**
     * Gets the entry version.
     *
     * @return the entry version.
     * @since 9.0.0
     */
    int getVersion();

    /**
     * Gets the entry expiration time.
     *
     * @return the entry expiration time.
     * @since 9.0.0
     */
    long getExpirationTime();

}
