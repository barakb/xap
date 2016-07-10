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

package com.j_spaces.kernel.pool;

import com.gigaspaces.time.SystemTime;

/**
 * A resource that can be traced back to it's allocation point. Helps to find leaks in code which
 * acquire a resource but do not return it back into the pool. <p> <b>Usage</b>: replace the {@link
 * Resource} with a {@link TraceableResource} e.g., <code>public myResource extends Resource { ...
 * }</code> would be replaced with <code>public myResource extends TraceableResource {...}</code>
 *
 * @author moran
 * @version 1.0
 * @since 6.0.2
 */
public abstract class TraceableResource extends Resource {
    /**
     * use -Ddebug.traceable-resource.timeout=5000 to set the resource release timeout in ms
     */
    private static final long RESOURCE_TIMEOUT = Integer.getInteger("debug.traceable-resource.timeout", 5000);

    /**
     * when this resource was last acquired
     */
    long _timestamp;

    /**
     * The throwable stack to trace the origin of this resource request
     */
    Throwable _stackTrace;

    @Override
    public void release() {
        _timestamp = 0;
        _stackTrace = null;

        super.release();
    }

    @Override
    public boolean acquire() {
        boolean acquired = super.acquire();

        //set timestamp & stack trace for acquired resource
        final long now = SystemTime.timeMillis();

        //If acquired, monitor it
        if (acquired) {
            _timestamp = now;
            _stackTrace = new Throwable("\n\tOrigin of resource request:");
        } else //else - already acquired, check it
        {
            failOnElapsedTimeout(now);
        }

        return acquired;
    }

    /**
     * Checks that this resource hasn't been held longer than the specified timeout limit.
     *
     * @param now current timestamp.
     * @throws ResourceTimeoutException if timeous has elapsed.
     */
    private void failOnElapsedTimeout(long now) {
        if (_timestamp > 0
                && (now - _timestamp > RESOURCE_TIMEOUT)) {
            throw new ResourceTimeoutException("Resource of type: "
                    + this.getClass().getName()
                    + " has exceeded its timeout of "
                    + RESOURCE_TIMEOUT + " ms"
                    + " by " + (now - _timestamp) + " ms"
                    , _stackTrace);
        }
    }

    /**
     * A resource timeout exception to be thrown if elapsed timeout was reached.
     */
    private static class ResourceTimeoutException extends RuntimeException {
        /**
         * default serial version uid
         */
        private static final long serialVersionUID = 1L;

        public ResourceTimeoutException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
