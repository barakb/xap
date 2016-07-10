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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.lrmi.nio.filters.IOFilterManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public interface IChannelWriter {

    public void writeBytesToChannelNoneBlocking(Writer.Context ctx, boolean restoreReadInterest)
            throws IOException;

    public void writeBytesToChannelBlocking(ByteBuffer dataBuffer)
            throws IOException, ClosedChannelException, SlowConsumerException;

    public void setFilterManager(IOFilterManager filterManager);

    public boolean isBlocking();

}