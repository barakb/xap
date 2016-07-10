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


package com.gigaspaces.lrmi.nio.filters;

import java.nio.ByteBuffer;

/**
 * Interface for block type network filters. Block network filter is filter that works in blocks. It
 * have network buffer size and packet buffer size that the define the max block size for each
 * direction.
 *
 * @author barak
 */
public interface IOBlockFilter extends IOFilter {

    /**
     * Initiation of handshake process.
     */
    void beginHandshake() throws IOFilterException;

    /**
     * When filter wish to do some calculation this is the way to handle the calculation code to the
     * filter container.
     *
     * @return task to run
     */
    Runnable getDelegatedTask();

    /**
     * Report the Hanshake status.
     *
     * @return hanshake status
     */
    IOFilterResult.HandshakeStatus getHandshakeStatus();

    /**
     * Wrap some bytes
     *
     * @param src source buffer
     * @param dst target buffer
     * @return filter result
     */
    IOFilterResult wrap(ByteBuffer src, ByteBuffer dst) throws IOFilterException;

    /**
     * Unwrap some bytes
     *
     * @param src source buffer
     * @param dst target buffer
     * @return filter result
     */
    IOFilterResult unwrap(ByteBuffer src, ByteBuffer dst) throws IOFilterException;

    /**
     * Return the application max block size
     *
     * @return application max block size
     */
    int getApplicationBufferSize();

    /**
     * Return the network max block size
     *
     * @return network max block size
     */
    int getPacketBufferSize();

}
