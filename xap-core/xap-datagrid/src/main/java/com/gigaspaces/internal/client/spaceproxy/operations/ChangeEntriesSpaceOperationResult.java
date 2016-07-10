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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ChangeEntriesSpaceOperationResult extends SpaceOperationResult {
    private static final long serialVersionUID = 1L;

    private ChangeResult<?> _changeResult;

    private int _numOfEntriesMatched;

    public ChangeEntriesSpaceOperationResult() {
    }

    public void processExecutionException() throws TransactionException, RemoteException, InterruptedException {
        final Exception executionException = getExecutionException();
        if (executionException == null)
            return;
        if (executionException instanceof TransactionException)
            throw (TransactionException) executionException;
        if (executionException instanceof RemoteException)
            throw (RemoteException) executionException;
        if (executionException instanceof InterruptedException)
            throw (InterruptedException) executionException;

        onUnexpectedException(executionException);
    }

    public void setChangeResult(ChangeResult<?> changeResult) {
        _changeResult = changeResult;
    }

    public ChangeResult<?> getChangeResult() {
        return _changeResult;
    }

    public void setNumOfEntriesMatched(int numOfEntriesMatched) {
        _numOfEntriesMatched = numOfEntriesMatched;
    }

    public int getNumOfEntriesMatched() {
        return _numOfEntriesMatched;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("updateResult", _changeResult);
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    private static final short FLAG_NUM_OF_ENTRIES_MATCHED = 1 << 0;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _changeResult);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v10_0_0)) {
            final short flags = buildFlags();
            out.writeShort(flags);
            if (flags != 0) {
                if (_numOfEntriesMatched > 0)
                    out.writeInt(_numOfEntriesMatched);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _changeResult = IOUtils.readObject(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v10_0_0)) {
            final short flags = in.readShort();
            if (flags != 0) {
                if ((flags & FLAG_NUM_OF_ENTRIES_MATCHED) != 0)
                    this._numOfEntriesMatched = in.readInt();
            }
        }
    }

    private short buildFlags() {
        short flags = 0;

        if (_numOfEntriesMatched > 0)
            flags |= FLAG_NUM_OF_ENTRIES_MATCHED;

        return flags;
    }

}
