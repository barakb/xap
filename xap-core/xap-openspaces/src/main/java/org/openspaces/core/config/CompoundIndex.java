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
 * Describes compound index
 *
 * @author Yechiel
 * @since 9.5
 */

public class CompoundIndex extends SpaceIndex {
    public static enum CompoundIndexTypes {
        BASIC, EXTENDED
    }

    private String[] paths;
    private CompoundIndexTypes compoundIndexType;

    public CompoundIndex() {
        super();
    }

    public CompoundIndex(String name, String[] paths, CompoundIndexTypes compoundIndexType) {
        super(name);
        this.paths = paths;
        this.compoundIndexType = compoundIndexType;
    }

    public CompoundIndex(String name, String[] paths, CompoundIndexTypes compoundIndexType, boolean unique) {
        super(name, unique);
        this.paths = paths;
        this.compoundIndexType = compoundIndexType;
    }

    public String[] getPaths() {
        return paths;
    }

    public CompoundIndexTypes getCompoundIndexType() {
        return compoundIndexType;
    }

}
