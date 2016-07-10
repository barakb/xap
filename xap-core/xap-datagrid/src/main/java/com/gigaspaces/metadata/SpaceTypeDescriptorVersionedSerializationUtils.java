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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.TypeDescVersionedSerializable;

import java.io.Serializable;

/**
 * Provided utility methods to serialize/deserialize space type descriptors. The serialiable form
 * contains a version header which allowes it to be deserialized to a version compatible space type
 * descriptor
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceTypeDescriptorVersionedSerializationUtils {

    /**
     * @return A serializable form of the given type descriptor that upon serialization will write a
     * version header
     */
    public static Serializable toSerializableForm(SpaceTypeDescriptor typeDescriptor) {
        return ((ITypeDesc) typeDescriptor).getVersionedSerializable();
    }

    /**
     * Reads the version header for the given serializable form and returned a SpaceTypeDescriptor
     * matching that version
     *
     * @return the original SpaceTypeDescriptor passed to the method toSerializableForm
     */
    public static SpaceTypeDescriptor fromSerializableForm(Serializable versionedSerializableForm) {
        if (!(versionedSerializableForm instanceof TypeDescVersionedSerializable))
            throw new IllegalArgumentException("wrapper must be an instance of " +
                    TypeDescVersionedSerializable.class.getName());

        return ((TypeDescVersionedSerializable) versionedSerializableForm).getTypeDesc();
    }

}
