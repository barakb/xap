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

package com.gigaspaces.internal.query.valuegetter;

import com.gigaspaces.server.ServerEntry;

/**
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceValueGetters {
    public static <T> T getEntryProperty(Object target, int propertyIndex) {
        ServerEntry entry = (ServerEntry) target;
        return (T) entry.getFixedPropertyValue(propertyIndex);
    }

    public static <T> T getEntryProperty(Object target, String propertyName) {
        ServerEntry entry = (ServerEntry) target;
        return (T) entry.getPropertyValue(propertyName);
    }

    public static <T> T getEntryPath(Object target, String path) {
        SpaceEntryPathGetter getter = new SpaceEntryPathGetter(path);
        return (T) getter.getValue((ServerEntry) target);
    }
}
