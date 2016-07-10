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

package com.gigaspaces.metrics;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;

/**
 * @author Barak Bar Orion
 * @since 10.1
 */
public class UdpConnection {

    private final InetSocketAddress address;
    private final DatagramSocket socket;
    private final Charset charset;
    private boolean opened;

    public UdpConnection(String host, int port, Charset charset) throws SocketException {
        this.address = new InetSocketAddress(host, port);
        this.charset = charset;
        this.socket = new DatagramSocket();
        this.opened = true;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public synchronized void send(String content) throws IOException {
        if (opened) {
            final byte[] data = content.getBytes(charset);
            socket.send(new DatagramPacket(data, data.length, address));
        }
    }

    public synchronized void close() {
        opened = false;
        socket.close();
    }
}
