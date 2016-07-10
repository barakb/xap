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

package com.gigaspaces.internal.server.space.redolog.storage;

/**
 * Extends the {@link IRedoLogFileStorage} interface by adding a non batch behavior of adding
 * packets to the end of the list.
 *
 * This is used in order to allow the ability to decorate a storage with the ability to store only
 * one packet which will be used for buffering of packets addition without the need to actually
 * support a single packet addition it in the storage provider
 *
 * Implementor should support concurrent readers or a single writer, in other words, the implementor
 * can assume access to this structure are guarded with a reader writer lock according to the
 * operation type
 *
 * @author eitany
 * @since 7.1
 */
public interface INonBatchRedoLogFileStorage<T>
        extends IRedoLogFileStorage<T> {
    /**
     * Add a single packet to the end of the storage list
     */
    void append(T replicationPacket) throws StorageException, StorageFullException;
}
