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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.utils.ObjectConverter;

import net.jini.space.InternalSpaceException;

import java.sql.SQLException;

/**
 * @author Niv Ingberg
 * @since 7.1.1
 */
@com.gigaspaces.api.InternalApi
public class ConvertedObjectWrapper {
    private final Object _convertedObject;

    private ConvertedObjectWrapper(Object value) {
        this._convertedObject = value;
    }

    public Object getValue() {
        return _convertedObject;
    }

    public static ConvertedObjectWrapper create(Object value, Class<?> type) {
        // If value is null or not string, no need to convert:
        if (value == null || !(value instanceof String))
            return new ConvertedObjectWrapper(value);

        // If target type is null it is impossible to convert:
        if (type == null)
            return null;

        // Convert:
        try {
            value = ObjectConverter.convert(value, type);
            return new ConvertedObjectWrapper(value);
        } catch (SQLException e) {
            throw new InternalSpaceException(e.getMessage(), e);
        }
    }
}
