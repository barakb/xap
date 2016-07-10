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

/**
 * Represents a mutable server entry
 *
 * @author Niv Ingberg.
 * @since 9.1
 */
public interface MutableServerEntry extends ServerEntry {
    /**
     * Sets the specified path's value
     *
     * @param path  Path pointing to the requested property.
     * @param value Value to set.
     */
    void setPathValue(String path, Object value);

    /**
     * Unsets the specified path. If the path points to a dynamic property or a map key it will be
     * removed, if it points to a fixed property it will be set to null, in that case, the target
     * path must point to a nullable property otherwise an error will occur.
     *
     * @param path Path pointing to the requested property.
     */
    void unsetPath(String path);
}
