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

/**
 * Provide an endless byte buffer like storage, which support single writer or multi readers
 * concurrently interacting with it
 *
 * @author eitany
 * @since 7.1
 */
public interface IByteBufferStorage {
    /**
     * @return a cursor over the storage, which is used to read and write data from and into the
     * storage This does not guarantee thread safety and should be used by a single thread at
     * anytime
     */
    IByteBufferStorageCursor getCursor() throws ByteBufferStorageException;

    /**
     * Clears the storage, all writer and readers are obsolete and should be reconstructed after
     * this call
     */
    void clear() throws ByteBufferStorageException;

    /**
     * Close the storage, after this call the storage is no longer useable
     */
    void close();

    /**
     * @return the storage name (i.e file name)
     */
    String getName();
}
