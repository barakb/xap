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


package com.gigaspaces.internal.client.utils;

import com.gigaspaces.internal.io.CompressedMarshObjectConvertor;
import com.gigaspaces.internal.io.ContextClassResolverCompressedMarshObjectConvertor;
import com.gigaspaces.internal.io.ContextClassResolverMarshObjectConvertor;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.io.MarshObjectConvertor;
import com.gigaspaces.internal.io.MarshObjectConvertorResource;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourceFactory;
import com.gigaspaces.internal.utils.pool.MemoryBoundedResourcePool;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;

/**
 * A utility that provides a way to serialize and deserialize an entry field
 *
 * @author Guy Korland
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class SerializationUtil {
    private static final SerializationAdapter _binaryAdapter =
            new PooledMarshalledObjectSerializationAdapter<MarshObjectConvertor>(ContextClassResolverMarshObjectConvertor.getFactory());
    private static final SerializationAdapter _compressedAdapter =
            new PooledMarshalledObjectSerializationAdapter<CompressedMarshObjectConvertor>(ContextClassResolverCompressedMarshObjectConvertor.getFactory());

    /**
     * Serialize a field value.
     *
     * @param value             field value.
     * @param serializationType from com.j_spaces.core.SerializationType
     * @return the same value if null, otherwise returns a serialized version of the value.
     */
    public static Object serializeFieldValue(Object value, StorageType storageType)
            throws IOException {
        if (value == null)
            return value;

        SerializationAdapter adapter = getSerializationAdapter(storageType);
        return adapter == null ? value : adapter.beforeSerialize(value);
    }

    /**
     * Deserialize a field value.
     *
     * @param value             field value.
     * @param serializationType from com.j_spaces.core.SerializationType
     * @return the same value if it's basic or null, otherwise returns a deserialized version of the
     * value.
     */
    public static Object deSerializeFieldValue(Object value, StorageType storageType)
            throws ClassNotFoundException, IOException {
        if (value == null)
            return value;

        SerializationAdapter adapter = getSerializationAdapter(storageType);
        return adapter == null ? value : adapter.afterDeserialize(value);
    }

    private static SerializationAdapter getSerializationAdapter(StorageType storageType) {
        switch (storageType) {
            case OBJECT:
                return null;
            case BINARY:
                return _binaryAdapter;
            case COMPRESSED:
                return _compressedAdapter;
            default:
                throw new IllegalArgumentException("Invalid storage Type : " + storageType);
        }
    }

    public static interface SerializationAdapter {
        Object beforeSerialize(Object value) throws IOException;

        Object afterDeserialize(Object value) throws IOException, ClassNotFoundException;
    }

    public static class PooledMarshalledObjectSerializationAdapter<T extends MarshObjectConvertorResource> implements SerializationAdapter {
        private final MemoryBoundedResourcePool<T> _convertorPool;

        public PooledMarshalledObjectSerializationAdapter(IMemoryAwareResourceFactory<T> factory) {
            final int maxResources = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_RESOURCE_COUNT_SIZE, SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_RESOURCE_COUNT_SIZE_DEFAULT);
            final int poolMemoryBounds = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_MEMORY_SIZE, SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_MEMORY_SIZE_DEFAULT);
            _convertorPool = new MemoryBoundedResourcePool<T>(factory, 0, maxResources, poolMemoryBounds);
        }

        @Override
        public Object beforeSerialize(Object value) throws IOException {
            T convertor = _convertorPool.getResource();
            try {
                return convertor.getMarshObject(value);
            } finally {
                _convertorPool.freeResource(convertor);
            }
        }

        @Override
        public Object afterDeserialize(Object value) throws IOException, ClassNotFoundException {
            T convertor = _convertorPool.getResource();
            try {
                return convertor.getObject((MarshObject) value);
            } finally {
                _convertorPool.freeResource(convertor);
            }
        }
    }
}
