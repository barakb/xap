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

package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.AddTypeIndexesResponseInfo;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;
import com.j_spaces.core.IJSpace;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class AddTypeIndexesTask extends SystemDistributedTask<AddTypeIndexesResult> {
    private static final long serialVersionUID = 1L;

    private AddTypeIndexesRequestInfo _actionInfo;

    public AddTypeIndexesTask() {
        super();
    }

    public AddTypeIndexesTask(AddTypeIndexesRequestInfo actionInfo) {
        this._actionInfo = actionInfo;
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _actionInfo;
    }

    @Override
    public AddTypeIndexesResult execute(IJSpace space, Transaction tx) throws Exception {
        AddTypeIndexesResponseInfo result = (AddTypeIndexesResponseInfo) super.execute(space, tx);

        if (result.getMetadataException() != null)
            throw result.getMetadataException();
        return result;
    }

    @Override
    public AddTypeIndexesResult reduce(List<AsyncResult<AddTypeIndexesResult>> results)
            throws Exception {
        return _actionInfo.reduce(results);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _actionInfo);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _actionInfo = IOUtils.readObject(in);
    }
}
