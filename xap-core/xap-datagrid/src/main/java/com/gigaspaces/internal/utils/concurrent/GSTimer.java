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

package com.gigaspaces.internal.utils.concurrent;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Timer that should be used all across GigaSpaces code. Add "GS-" prefix to all the threads names.
 *
 * @author Guy
 * @see GSThread
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class GSTimer extends Timer {

    final static private AtomicInteger nextSerialNumber = new AtomicInteger();

    public GSTimer() {
        super("GS-Timer-" + nextSerialNumber.getAndIncrement());
    }


    public GSTimer(String name) {
        super("GS-" + name);
    }
}
