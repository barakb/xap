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

package com.gigaspaces.internal.cluster.node.impl.processlog.globalorder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse
        extends GlobalOrderProcessLogHandshakeResponse {
    private static final long serialVersionUID = 1L;
    private long _lastKeyInBacklog;

    public GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse() {
    }

    public GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse(GlobalOrderProcessLogHandshakeResponse baseResponse) {
        super(baseResponse.getLastProcessedKey());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(_lastKeyInBacklog);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _lastKeyInBacklog = in.readLong();
    }

    public void setLastKeyInBacklog(long lastKeyInBacklog) {
        _lastKeyInBacklog = lastKeyInBacklog;
    }

    public long getLastKeyInBacklog() {
        return _lastKeyInBacklog;
    }
}
