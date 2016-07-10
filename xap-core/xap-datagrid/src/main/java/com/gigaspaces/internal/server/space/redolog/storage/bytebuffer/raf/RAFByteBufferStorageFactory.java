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

package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.raf;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ByteBufferStorageException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.IByteBufferStorage;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.IByteBufferStorageFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link IByteBufferStorageFactory} implementation that provides {@link RAFByteBufferStorage}
 * instances
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class RAFByteBufferStorageFactory
        implements IByteBufferStorageFactory {

    private final String _fileName;
    private AtomicInteger _counter = new AtomicInteger(0);

    public RAFByteBufferStorageFactory(String fileName) {
        this._fileName = fileName;
    }

    public IByteBufferStorage createStorage() throws ByteBufferStorageException {
        int index = _counter.getAndIncrement();
        return new RAFByteBufferStorage(_fileName + "_" + index);
    }

}
