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

import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public interface IOFilterManager {

    public void writeBytesNonBlocking(Writer.Context ctx) throws IOException, IOFilterException;

    public byte[] handleNoneBlockingContant(Reader.Context ctx, byte[] bytes)
            throws IOException, IOFilterException;

    public byte[] handleBlockingContant(byte[] buffer, int slowConsumerTimeout)
            throws ClosedChannelException, IOException, IOFilterException;

    public void writeBytesBlocking(ByteBuffer dataBuffer)
            throws ClosedChannelException, SlowConsumerException, IOException, IOFilterException;

    public void beginHandshake() throws IOFilterException, IOException;

}