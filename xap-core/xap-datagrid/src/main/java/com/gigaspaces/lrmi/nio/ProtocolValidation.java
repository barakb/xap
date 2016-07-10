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

import com.gigaspaces.time.SystemTime;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

/**
 * @author Dan Kilman
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class ProtocolValidation {

    final public static String PROTOCOL_STRING = "gigaspaces-lrmi-protocol";

    public static int getProtocolHeaderBytesLength() {
        return getProtocolHeaderBytes().length;
    }

    public static byte[] getProtocolHeaderBytes() {
        return PROTOCOL_STRING.getBytes(Charset.forName("UTF-8"));
    }

    public static void writeProtocolValidationHeader(SocketChannel socketChannel, long timeout) throws IOException {
        long end;
        if (timeout == Long.MAX_VALUE)
            end = timeout;
        else {
            end = SystemTime.timeMillis() + timeout;
            if (end <= 0)
                end = Long.MAX_VALUE;
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(ProtocolValidation.getProtocolHeaderBytes());
        while (byteBuffer.hasRemaining() && SystemTime.timeMillis() < end) {
            int writtenBytes = socketChannel.write(byteBuffer);
            if (writtenBytes == 0)
                Thread.yield();
        }

        if (byteBuffer.hasRemaining())
            throw new SocketTimeoutException("Timed out while writing protocol header. timeout=" + timeout);
    }

}
