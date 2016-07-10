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

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

@com.gigaspaces.api.InternalApi
public class NIOUtils {
    public static String getSocketDisplayString(SocketChannel channel) {
        Socket socket = channel.socket();
        String identifier = "disconnected";
        if (socket != null) {
            InetAddress localAddress = socket.getLocalAddress();
            String localAddressStr = localAddress == null ? "null" : localAddress.toString() + ":" + socket.getLocalPort();
            InetAddress remoteAddress = socket.getInetAddress();
            String remoteAddressStr = remoteAddress == null ? "null" : remoteAddress.toString() + ":" + socket.getPort();
            identifier = localAddressStr + "->" + remoteAddressStr;
        }
        return identifier;
    }
}
