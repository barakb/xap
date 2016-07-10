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

package org.openspaces.remoting;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy (shay.banon)
 * @deprecated
 */
@Deprecated
public class HashedEventDrivenSpaceRemotingEntry extends EventDrivenSpaceRemotingEntry
        implements HashedSpaceRemotingEntry {

    private static final long serialVersionUID = -6752531933557296453L;

    public RemotingUtils.MethodHash methodHash;

    public RemotingUtils.MethodHash getMethodHash() {
        return methodHash;
    }

    public HashedSpaceRemotingEntry buildInvocation(String lookupName, String methodName, RemotingUtils.MethodHash methodHash, Object[] arguments) {
        setResult(null);
        setException(null);
        setInvocation(Boolean.TRUE);
        setLookupName(lookupName);
        setMethodName(methodName);
        this.methodHash = methodHash;
        setArguments(arguments);
        return this;
    }

    @Override
    public SpaceRemotingEntry buildResultTemplate() {
        methodHash = null;
        return super.buildResultTemplate();
    }

    @Override
    public SpaceRemotingEntry buildResult(Throwable e) {
        methodHash = null;
        return super.buildResult(e);
    }

    @Override
    public HashedSpaceRemotingEntry buildResult(Object result) {
        methodHash = null;
        return (HashedSpaceRemotingEntry) super.buildResult(result);
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        if (in.readBoolean()) {
            methodHash = new RemotingUtils.MethodHash();
            methodHash.readExternal(in);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if (methodHash == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            methodHash.writeExternal(out);
        }
    }
}
