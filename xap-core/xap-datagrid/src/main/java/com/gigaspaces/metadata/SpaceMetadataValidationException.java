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


package com.gigaspaces.metadata;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;

/**
 * Thrown to indicate an invalid metadata on a user object.
 *
 * @author Guy Korland
 * @version 7.0
 */

public class SpaceMetadataValidationException extends SpaceMetadataException {
    private static final long serialVersionUID = 1L;

    public SpaceMetadataValidationException(String typeName, String errorMessage) {
        super("Invalid metadata for class [" + typeName + "]: " + errorMessage);
    }

    public SpaceMetadataValidationException(Class<?> type, String errorMessage) {
        super("Invalid metadata for class [" + type.getName() + "]: " + errorMessage);
    }

    public SpaceMetadataValidationException(Class<?> type, SpacePropertyInfo property, String errorMessage) {
        super("Invalid metadata for class [" + type.getName() +
                "], property [" + property.getName() + "]: " + errorMessage);
    }

    public SpaceMetadataValidationException(Class<?> type, SpacePropertyInfo property, String errorMessage, Throwable cause) {
        super("Invalid metadata for class [" + type.getName() +
                "], property [" + property.getName() + "]: " + errorMessage, cause);
    }

    public SpaceMetadataValidationException(Class<?> type, String errorMessage, Throwable cause) {
        super("Invalid metadata for class [" + type.getName() +
                "]: " + errorMessage, cause);
    }

    public SpaceMetadataValidationException(String typeName, PropertyInfo property, String errorMessage) {
        super("Invalid metadata for class [" + typeName +
                "], property [" + property.getName() + "]: " + errorMessage);
    }

    public SpaceMetadataValidationException(String typeName, String propertyName, String errorMessage) {
        super("Invalid metadata for class [" + typeName +
                "], property [" + propertyName + "]: " + errorMessage);
    }
}
