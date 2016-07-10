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


package com.gigaspaces.internal.io;

import com.gigaspaces.internal.utils.pool.IMemoryAwareResourceFactory;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourcePool;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Adds class annotation to the converted object.
 *
 * @author anna
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class ContextClassResolverMarshObjectConvertor
        extends MarshObjectConvertor {

    private static ContextClassResolverMarshObjectConvertorFactory _factory;

    public ContextClassResolverMarshObjectConvertor() {
    }

    public ContextClassResolverMarshObjectConvertor(
            ISmartLengthBasedCacheCallback cacheCallback) {
        super(cacheCallback);
    }

    /*
     * @see com.j_spaces.kernel.lrmi.MarshObjectConvertor#getObjectInputStream(java.io.InputStream)
	 */
    @Override
    protected ObjectInputStream getObjectInputStream(
            InputStream is) throws IOException {
        return new ContextClassResolverObjectInputStream(is);
    }

    /**
     *
     * @return
     */
    public static IMemoryAwareResourceFactory<MarshObjectConvertor> getFactory() {
        if (_factory == null)
            _factory = new ContextClassResolverMarshObjectConvertorFactory();

        return _factory;
    }

    /**
     * Factory for creating AnnotatedMarshObjectConvertor
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class ContextClassResolverMarshObjectConvertorFactory
            extends
            MarshObjectConvertor.MarshObjectConvertorFactory {

        /**
         *
         */
        public MarshObjectConvertor allocate() {
            return new ContextClassResolverMarshObjectConvertor();
        }

        @Override
        public MarshObjectConvertor allocate(
                IMemoryAwareResourcePool resourcePool) {
            return new ContextClassResolverMarshObjectConvertor(SmartLengthBasedCache.toCacheCallback(resourcePool));
        }
    }

}
