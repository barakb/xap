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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.space.responses.RegisterTypeDescriptorResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class RegisterTypeDescriptorRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;

    public ITypeDesc typeDescriptor;

    /**
     * Required for Externalizable
     */
    public RegisterTypeDescriptorRequestInfo() {
    }

    public RegisterTypeDescriptorRequestInfo(ITypeDesc typeDescriptor) {
        this.typeDescriptor = typeDescriptor;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, typeDescriptor);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.typeDescriptor = IOUtils.readObject(in);
    }

    public RegisterTypeDescriptorResponseInfo reduce(List<AsyncResult<RegisterTypeDescriptorResponseInfo>> results)
            throws Exception {
        RegisterTypeDescriptorResponseInfo finalResult = null;

        for (AsyncResult<RegisterTypeDescriptorResponseInfo> result : results) {
            if (result.getException() != null)
                throw result.getException();

            if (finalResult == null) {
                finalResult = result.getResult();
            } else {
                //TODO validate that types are the same on all spaces
            }
        }

        return finalResult;
    }
}
