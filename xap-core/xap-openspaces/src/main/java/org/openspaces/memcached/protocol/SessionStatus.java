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

package org.openspaces.memcached.protocol;

import java.io.Serializable;

/**
 * Class for holding the current session status.
 */
public final class SessionStatus implements Serializable {

    private static final long serialVersionUID = 8948155047611447607L;

    /**
     * Possible states that the current session is in.
     */
    public static enum State {
        WAITING_FOR_DATA,
        READY,
        PROCESSING,
        PROCESSING_MULTILINE,
    }

    // the state the session is in
    public State state;

    // if we are waiting for more data, how much?
    public int bytesNeeded;

    // the current working command
    public CommandMessage cmd;


    public SessionStatus() {
        ready();
    }

    public SessionStatus ready() {
        this.cmd = null;
        this.bytesNeeded = -1;
        this.state = State.READY;

        return this;
    }

    public SessionStatus processing() {
        this.state = State.PROCESSING;

        return this;
    }

    public SessionStatus processingMultiline() {
        this.state = State.PROCESSING_MULTILINE;

        return this;
    }

    public SessionStatus needMore(int size, CommandMessage cmd) {
        this.cmd = cmd;
        this.bytesNeeded = size;
        this.state = State.WAITING_FOR_DATA;

        return this;
    }

}
