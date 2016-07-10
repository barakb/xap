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
 * A configuration object for {@link ByteBufferRedoLogFileStorage}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ByteBufferRedoLogFileConfig<T> {
    private static final int KILOBYTE = 1024;
    private static final int MEGABYTE = 1024 * KILOBYTE;
    public static final long UNLIMITED = -1;

    public static final int DEFAULT_BUFFER_SIZE = 16 * KILOBYTE;
    public static final int DEFAULT_MAX_BUFFER_SIZE = 256 * KILOBYTE;
    public static final long DEFAULT_SWAP_SIZE = UNLIMITED;
    public static final long DEFAULT_SEGMENT_SIZE = 10 * MEGABYTE;
    public static final int DEFAULT_MAX_SCAN = 50 * KILOBYTE;
    public static final int DEFAULT_MAX_STORAGE_CURSORS = 10;

    private int _writerBufferSize = DEFAULT_BUFFER_SIZE;
    private int _writerMaxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
    private long _maxSwapSize = DEFAULT_SWAP_SIZE;
    private long _maxSizePerSegment = DEFAULT_SEGMENT_SIZE;
    private int _maxScanLength = DEFAULT_MAX_SCAN;
    private int _maxOpenStorageCursors = DEFAULT_MAX_STORAGE_CURSORS;
    private IPacketStreamSerializer<T> _packetStreamSerializer = new DefaultPacketStreamSerializer<T>();


    public int getWriterBufferSize() {
        return _writerBufferSize;
    }

    public int getWriterMaxBufferSize() {
        return _writerMaxBufferSize;
    }

    /**
     * Sets the write buffer init size in bytes
     */
    public void setWriterBufferSize(int writerBufferSize) {
        this._writerBufferSize = writerBufferSize;
    }

    /**
     * Sets the maximum size if the write buffer in bytes
     */
    public void setWriterMaxBufferSize(int writerMaxBufferSize) {
        this._writerMaxBufferSize = writerMaxBufferSize;
        if (_writerMaxBufferSize < this._writerBufferSize)
            _writerBufferSize = _writerMaxBufferSize;
    }

    /**
     * Sets the maximum size in bytes of the entire storage, -1 states unlimited
     */
    public void setMaxSwapSize(long maxSwapSize) {
        this._maxSwapSize = maxSwapSize;
    }

    public long getMaxSwapSize() {
        return _maxSwapSize;
    }

    /**
     * Sets the maximum size of each segment of the storage
     *
     * @param maxSizePerSegment maximum size in bytes, must be a positive non zero number
     */
    public void setMaxSizePerSegment(long maxSizePerSegment) {
        this._maxSizePerSegment = maxSizePerSegment;
    }

    public long getMaxSizePerSegment() {
        return _maxSizePerSegment;
    }

    public void setMaxScanLength(int maxScanLength) {
        this._maxScanLength = maxScanLength;
    }

    public int getMaxScanLength() {
        return _maxScanLength;
    }

    /**
     * Set max allows open storage readers (usually translate to maximum allowed open file
     * descriptors)
     */
    public void setMaxOpenStorageCursors(int maxOpenStorageReaders) {
        this._maxOpenStorageCursors = maxOpenStorageReaders;
    }

    public int getMaxOpenStorageCursors() {
        return _maxOpenStorageCursors;
    }

    public IPacketStreamSerializer<T> getPacketStreamSerializer() {
        return _packetStreamSerializer;
    }

    public void setPacketStreamSerializer(IPacketStreamSerializer<T> packetStreamSerializer) {
        this._packetStreamSerializer = packetStreamSerializer;
    }


}
