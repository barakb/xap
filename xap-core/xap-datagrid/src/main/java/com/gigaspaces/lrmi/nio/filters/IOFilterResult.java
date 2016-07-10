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

package com.gigaspaces.lrmi.nio.filters;

@com.gigaspaces.api.InternalApi
public class IOFilterResult {
    public enum HandshakeStatus {
        FINISHED,
        NEED_TASK,
        NEED_UNWRAP,
        NEED_WRAP,
        NOT_HANDSHAKING
    }

    public enum Status {
        BUFFER_OVERFLOW,
        BUFFER_UNDERFLOW,
        CLOSED,
        OK
    }

    private HandshakeStatus handshakeStatus;
    private Status status;
    private int bytesConsumed;
    private int bytesProduced;


    public IOFilterResult(Status status, HandshakeStatus handshakeStatus,
                          int bytesConsumed, int bytesProduced) {
        this.status = status;
        this.handshakeStatus = handshakeStatus;
        this.bytesConsumed = bytesConsumed;
        this.bytesProduced = bytesProduced;
    }

    public HandshakeStatus getHandshakeStatus() {
        return handshakeStatus;
    }

    public void setHandshakeStatus(HandshakeStatus handshakeStatus) {
        this.handshakeStatus = handshakeStatus;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getBytesConsumed() {
        return bytesConsumed;
    }

    public int getBytesProduced() {
        return bytesProduced;
    }


}
