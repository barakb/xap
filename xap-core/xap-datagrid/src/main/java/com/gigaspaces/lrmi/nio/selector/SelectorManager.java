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

package com.gigaspaces.lrmi.nio.selector;

import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.lrmi.nio.Pivot;
import com.gigaspaces.lrmi.nio.selector.handler.AcceptSelectorThread;
import com.gigaspaces.lrmi.nio.selector.handler.ReadSelectorThread;
import com.gigaspaces.lrmi.nio.selector.handler.WriteSelectorThread;
import com.j_spaces.kernel.ManagedRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;

/**
 * Event queue for I/O events raised by a selector. This class receives the lower level events
 * raised by a Selector and dispatches them to the appropriate handler. The SelectorThread class
 * performs a similar task. In particular:
 *
 * - Listens for connection requests, accepts them and creates new connections in the Pivot Channels
 * Table.
 *
 * - Selects a set of channels available for read. If a channel is available for read, it creates a
 * bus packet with the channel info and dispatches it to a worker thread or handling.
 *
 * @author Guy Korland
 * @since 6.0.4, 6.5
 **/
@com.gigaspaces.api.InternalApi
public class SelectorManager extends ManagedRunnable {
    final private Pivot _pivot;

    final private ReadSelectorThread[] _readSelectorThread;
    final private WriteSelectorThread[] _writeSelectorThread;
    final private AcceptSelectorThread _acceptSelectorThread;

    final private String _port;
    final private String _hostName;


    public SelectorManager(Pivot pivot, String hostName, String port, int readSelectorThreads) throws IOException {
        _pivot = pivot;
        _hostName = hostName;
        _port = port;
        _readSelectorThread = new ReadSelectorThread[readSelectorThreads];
        _writeSelectorThread = new WriteSelectorThread[readSelectorThreads];
        try {
            for (int i = 0; i < readSelectorThreads; ++i) {
                _readSelectorThread[i] = new ReadSelectorThread(_pivot);
                GSThread readThread = new GSThread(_readSelectorThread[i], "LRMI-Selector-Read-Thread-" + i);
                readThread.setDaemon(true);
                readThread.start();

                _writeSelectorThread[i] = new WriteSelectorThread(_pivot);
                GSThread writeThread = new GSThread(_writeSelectorThread[i], "LRMI-Selector-Write-Thread-" + i);
                writeThread.setDaemon(true);
                writeThread.start();
            }

            _acceptSelectorThread = new AcceptSelectorThread(_pivot, _readSelectorThread, _hostName, _port);

            GSThread acceptThread = new GSThread(_acceptSelectorThread, "LRMI-Selector-Accept-Thread-" + _port);
            acceptThread.setDaemon(true);
            acceptThread.start(); // start the accept thread
        } catch (IOException e) {
            waitWhileFinish();
            throw e;
        }
    }

    @Override
    protected void waitWhileFinish() {
        // accept handler
        if (_acceptSelectorThread != null)
            _acceptSelectorThread.requestShutdown();

        // close readers
        for (ReadSelectorThread selectorThread : _readSelectorThread) {
            if (selectorThread != null)
                selectorThread.requestShutdown();
        }

        // close writers
        for (WriteSelectorThread selectorThread : _writeSelectorThread) {
            if (selectorThread != null)
                selectorThread.requestShutdown();
        }
    }

    public InetSocketAddress getBindInetSocketAddress() {
        return _acceptSelectorThread.getBindInetSocketAddress();
    }

    public WriteSelectorThread getWriteHandler(SelectableChannel channel) {
        return _writeSelectorThread[Math.abs(System.identityHashCode(channel) % _writeSelectorThread.length)];
    }

    public int getPort() {
        return _acceptSelectorThread.getPort();
    }

    public String getHostName() {
        return _acceptSelectorThread.getHostName();
    }
}