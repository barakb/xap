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

package com.gigaspaces.internal.cluster.node.impl.packets.data.errors;

import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeErrorResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataProducer;
import com.gigaspaces.internal.server.space.SpaceEngine;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class UnknownConsumeErrorResult
        extends AbstractDataConsumeErrorResult {

    private static final long serialVersionUID = 1L;

    private Throwable _error;

    public UnknownConsumeErrorResult() {
    }

    public UnknownConsumeErrorResult(Throwable error) {
        this._error = error;
    }

    @Override
    public AbstractDataConsumeFix createFix(SpaceEngine spaceEngine, ReplicationPacketDataProducer producer, IExecutableReplicationPacketData errorData) {
        return producer.createFixForUnknownError(_error);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _error = (Throwable) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_error);
    }

    public Exception toException() {
        if (_error instanceof Exception)
            return (Exception) _error;

        return new Exception(_error.getMessage(), _error);
    }

    public boolean sameFailure(IDataConsumeResult otherResult) {
        if (!(otherResult instanceof UnknownConsumeErrorResult))
            return false;

        Throwable otherError = ((UnknownConsumeErrorResult) otherResult)._error;

        if (otherError == _error)
            return true;

        if (_error == null || otherError == null)
            return false;

        if (!otherError.getClass().equals(_error.getClass()))
            return false;
        if (!otherError.getMessage().equals(_error.getMessage()))
            return false;

        return otherError.toString().equals(_error.toString());
    }

}
