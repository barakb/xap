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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileProcessResult
        implements IProcessResult {

    private static final long serialVersionUID = 1L;
    public static final MultiBucketSingleFileProcessResult OK = new MultiBucketSingleFileProcessResult(true);
    public static final MultiBucketSingleFileProcessResult PENDING = new MultiBucketSingleFileProcessResult(null);

    private boolean _processed;
    private Throwable _error;

    public MultiBucketSingleFileProcessResult() {
    }

    public MultiBucketSingleFileProcessResult(boolean processed) {
        _processed = processed;
    }

    public MultiBucketSingleFileProcessResult(Throwable error) {
        _processed = false;
        _error = error;
    }

    public boolean isProcessed() {
        return _processed;
    }

    public Throwable getError() {
        return _error;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(_processed);
        if (!_processed) {
            IOUtils.writeObject(out, _error);
        }
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _processed = in.readBoolean();
        if (!_processed) {
            _error = IOUtils.readObject(in);
        }
    }

    //This is relevant to the processing side only, used by parallel batch processing and should
    //never reach as true state to the client
    public boolean isPending() {
        return !isProcessed() && getError() == null;
    }

}
