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

package org.openspaces.persistency.cassandra.meta.types;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;

/**
 * Variation on hector's type inferring serializer to also include support for big decimal type,
 * float type and double type, to avoid possible NullPointerException and to use the map based
 * {@link SerializerProvider} instead of hectors {@link SerializerTypeInferer}.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CustomTypeInferringSerializer<T>
        extends AbstractSerializer<T>
        implements Serializer<T> {

    @SuppressWarnings("rawtypes")
    private static final CustomTypeInferringSerializer INSTANCE = new CustomTypeInferringSerializer();

    @SuppressWarnings("unchecked")
    public static <T> CustomTypeInferringSerializer<T> get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(T obj) {
        return SerializerProvider.getSerializer(obj.getClass()).toByteBuffer(obj);
    }

    @Override
    public T fromByteBuffer(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "The type inferring serializer can only be used for data going to the database, and not data coming from the database");
    }

}
