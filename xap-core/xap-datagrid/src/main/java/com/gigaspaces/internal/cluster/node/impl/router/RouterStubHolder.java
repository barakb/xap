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

package com.gigaspaces.internal.cluster.node.impl.router;

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class RouterStubHolder
        implements Externalizable {
    private static final long serialVersionUID = 1L;
    private IRouterStub _stub;
    private ReplicationEndpointDetails _myEndpointDetails;

    public RouterStubHolder() {
    }

    public RouterStubHolder(IRouterStub stub, ReplicationEndpointDetails myEndpointDetails) {
        _stub = stub;
        _myEndpointDetails = myEndpointDetails;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _stub);
        _myEndpointDetails.writeExternal(out);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _stub = IOUtils.readObject(in);
        _myEndpointDetails = new ReplicationEndpointDetails();
        _myEndpointDetails.readExternal(in);
    }

    public IRouterStub getStub() {
        return _stub;
    }

    public ReplicationEndpointDetails getMyEndpointDetails() {
        return _myEndpointDetails;
    }

    @Override
    public String toString() {
        return "RouterStubHolder [getMyEndpointDetails()=" + getMyEndpointDetails() + "]";
    }


}
