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

package org.openspaces.core.config;

/**
 * Super class for space indexes
 *
 * @author anna
 * @since 8.0
 */
public class SpaceIndex {

    private String path;
    private boolean _unique;

    public SpaceIndex() {
        super();
    }

    public SpaceIndex(String path) {
        super();
        this.path = path;
    }

    public SpaceIndex(boolean unique) {
        super();
        _unique = unique;
    }

    public SpaceIndex(String path, boolean unique) {
        super();
        _unique = unique;
        this.path = path;

    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isUnique() {
        return _unique;
    }

    public void setUnique() {
        _unique = true;
    }
}
