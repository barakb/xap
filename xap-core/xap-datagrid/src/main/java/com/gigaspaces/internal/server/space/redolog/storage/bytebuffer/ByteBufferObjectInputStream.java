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

package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.StreamCorruptedException;

/**
 * Used by {@link PacketSerializer} as {@link ObjectInputStream}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ByteBufferObjectInputStream
        extends ObjectInputStream {

    public ByteBufferObjectInputStream(InputStream in) throws IOException {
        super(in);
    }


    @Override
    protected void readStreamHeader() throws IOException,
            StreamCorruptedException {
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException,
            ClassNotFoundException {
        return ClassLoaderHelper.loadClass(desc.getName());
    }
}
