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

/**
 * @author Dan Kilman
 * @since 9.5
 */
public interface SystemRequestHandler {
    interface RequestCodeHeader {
        public static final int WATCHDOG_MONITOR_HEADER_V95X = -2;
        public static final int WATCHDOG_MONITOR_HEADER = -3;
    }

    interface SystemRequestContext {
        int getRequestDataLength();

        void prepare(byte[] result);

        Runnable getResponseTask(Pivot pivot, ChannelEntry channelEntry, long requestReadStartTimeStamp);
    }

    boolean handles(int requestCode);

    SystemRequestContext getRequestContext(int requestCode);
}
