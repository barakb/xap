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


package org.openspaces.remoting;

import org.springframework.remoting.RemoteAccessException;

/**
 * A Space remoting exception caused by a timeout waiting for a result.
 *
 * @author kimchy
 */
public class RemoteTimeoutException extends RemoteAccessException {

    private static final long serialVersionUID = -392552156381478754L;

    private long timeout;

    public RemoteTimeoutException(String message, long timeout) {
        super(message + ", timeout [" + timeout + "ms]");
        this.timeout = timeout;
    }

    public RemoteTimeoutException(String message, long timeout, Throwable cause) {
        super(message + ", timeout [" + timeout + "ms]", cause);
        this.timeout = timeout;
    }

    public long getTimeout() {
        return timeout;
    }
}
