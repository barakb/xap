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

package com.gigaspaces.lrmi.nio.selector.handler.client;

import com.gigaspaces.lrmi.nio.IWriteInterestManager;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * @author Asy Ronen
 * @since 6.5
 */
public interface Context extends IWriteInterestManager {
    void setSelectionKey(SelectionKey key);

    SelectionKey getSelectionKey();

    void handleSetReadInterest();

    void handleRead() throws IOException;

    void handleWrite() throws IOException;

    /**
     * Called by the Selector on unregister.
     */
    void close();

    void close(Throwable exception);

    void closeAndDisconnect();
}