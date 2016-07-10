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

package com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderBacklogHandshakeRequest;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest extends MultiSourceSingleFileBacklogHandshakeRequest {
    private static final long serialVersionUID = -1057538038099950262L;

    private MultiSourceSingleFileReliableAsyncState _reliableAsyncState;

    /**
     * For Externalizable.
     */
    public MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest() {
        super();
    }

    public MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest(
            GlobalOrderBacklogHandshakeRequest handshakeRequest,
            MultiSourceSingleFileReliableAsyncState reliableAsyncState) {
        super(handshakeRequest.isFirstHandshake(), handshakeRequest.getLastConfirmedKey());
        _reliableAsyncState = reliableAsyncState;
    }

    public MultiSourceSingleFileReliableAsyncState getReliableAsyncState() {
        return _reliableAsyncState;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _reliableAsyncState = new MultiSourceSingleFileReliableAsyncState();
        _reliableAsyncState.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        _reliableAsyncState.writeExternal(out);
    }

}
