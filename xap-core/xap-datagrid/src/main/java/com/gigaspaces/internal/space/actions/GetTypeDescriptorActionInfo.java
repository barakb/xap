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

package com.gigaspaces.internal.space.actions;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class GetTypeDescriptorActionInfo extends AbstractSpaceActionInfo {
    private static final long serialVersionUID = 1L;

    // Request members:
    public String typeName;
    // Response members:
    public ITypeDesc resultTypeDescriptor;

    /**
     * Required for Externalizable
     */
    public GetTypeDescriptorActionInfo() {
    }

    public GetTypeDescriptorActionInfo(String typeName) {
        this.typeName = typeName;
    }

    @Override
    protected void writeRequest(ObjectOutput out)
            throws IOException {
        super.writeRequest(out);

        IOUtils.writeRepetitiveString(out, typeName);
    }

    @Override
    protected void readRequest(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readRequest(in);

        this.typeName = IOUtils.readRepetitiveString(in);
    }

    @Override
    protected void writeResponse(ObjectOutput out)
            throws IOException {
        super.writeResponse(out);

        IOUtils.writeObject(out, resultTypeDescriptor);
    }

    @Override
    protected void readResponse(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readResponse(in);

        this.resultTypeDescriptor = IOUtils.readObject(in);
    }

    public GetTypeDescriptorActionInfo reduce(List<AsyncResult<GetTypeDescriptorActionInfo>> results) {
        GetTypeDescriptorActionInfo finalResult = null;

        for (AsyncResult<GetTypeDescriptorActionInfo> result : results) {
            //ignore failed targets
            if (result.getException() != null)
                continue;

            if (finalResult == null) {
                finalResult = result.getResult();
            } else {
                // TODO validate that types are the same on all spaces
            }
        }

        if (finalResult == null)
            finalResult = new GetTypeDescriptorActionInfo(this.typeName);

        return finalResult;
    }
}
