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

package com.gigaspaces.internal.space.requests;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.RegisterReplicationLocalViewResponseInfo;
import com.gigaspaces.internal.transport.ITemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class RegisterReplicationLocalViewRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;

    public ITemplatePacket[] templates;
    public RouterStubHolder viewStub;
    public int batchSize;
    public long batchTimeout;

    /**
     * Required for Externalizable
     */
    public RegisterReplicationLocalViewRequestInfo() {
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        out.writeInt(templates.length);
        for (int i = 0; i < templates.length; i++)
            IOUtils.writeObject(out, templates[i]);
        IOUtils.writeObject(out, viewStub);
        out.writeInt(batchSize);
        out.writeLong(batchTimeout);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        int length = in.readInt();
        this.templates = new ITemplatePacket[length];
        for (int i = 0; i < length; i++)
            templates[i] = IOUtils.readObject(in);
        this.viewStub = IOUtils.readObject(in);
        this.batchSize = in.readInt();
        this.batchTimeout = in.readLong();
    }

    public RegisterReplicationLocalViewResponseInfo reduce(List<AsyncResult<RegisterReplicationLocalViewResponseInfo>> results)
            throws Exception {
        for (AsyncResult<RegisterReplicationLocalViewResponseInfo> result : results) {
            if (result.getException() != null)
                throw result.getException();
            RegisterReplicationLocalViewResponseInfo responseInfo = result.getResult();
            if (responseInfo.exception != null)
                throw responseInfo.exception;
        }

        return null;
    }
}
