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

package com.j_spaces.core.cache;

/**
 * @author Yechiel Fefer
 * @version 1.0
 * @since 10.1
 */

import java.util.concurrent.atomic.AtomicLong;

/**
 * handle sequenceNumbers creation/getting/updates
 */
@com.gigaspaces.api.InternalApi
public class SequenceNumberGenerator {
    private final AtomicLong _seed;


    SequenceNumberGenerator() {
        _seed = new AtomicLong();
    }

    Long getNext() {
        return _seed.incrementAndGet();
    }

    Long getCurrent() {
        return _seed.get();
    }

    void updateIfGreater(long newCurrent) {
        //note - called ONLY from replication. replication is mostly (execution level) single threaded
        while (true) {
            long cur = getCurrent();
            if (cur >= newCurrent)
                return;
            if (_seed.compareAndSet(cur, newCurrent))
                return;
        }
    }
}
