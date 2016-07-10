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

import com.gigaspaces.time.SystemTime;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class TimedCompetitionExecutor<T extends CompetitiveTask> extends CompetitionExecutor<T> {
    private final long _deadline;

    public TimedCompetitionExecutor(T[] competitors, long timeout, String name, IAsyncHandlerProvider asyncHandlerProvider, long idleDelay) {
        super(asyncHandlerProvider);
        if (timeout <= 0)
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        this._deadline = SystemTime.timeMillis() + timeout;

        start(competitors, idleDelay, name);
    }

    @Override
    protected CompetitorWrapper wrapCompetitor(T competitor) {
        return new TimedCompetitorWrapper(competitor);
    }

    private class TimedCompetitorWrapper extends CompetitorWrapper {
        public TimedCompetitorWrapper(T competitor) {
            super(competitor);
        }

        protected boolean isLastIteration() {
            long currTime = SystemTime.timeMillis();
            return (currTime > _deadline);
        }
    }
}
