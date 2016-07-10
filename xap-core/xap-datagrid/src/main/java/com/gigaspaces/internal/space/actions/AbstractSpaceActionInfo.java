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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
public abstract class AbstractSpaceActionInfo implements SpaceRequestInfo, SpaceResponseInfo {
    private static final long serialVersionUID = 1L;

    private boolean _isResponse;
    private SpaceContext _spaceContext;

    /**
     * Required for Externalizable
     */
    public AbstractSpaceActionInfo() {
    }

    public SpaceContext getSpaceContext() {
        return _spaceContext;
    }

    public void setSpaceContext(SpaceContext spaceContext) {
        _spaceContext = spaceContext;
    }

    public void setResponse() {
        this._isResponse = true;
    }

    public void writeExternal(ObjectOutput out)
            throws IOException {
        out.writeBoolean(this._isResponse);
        if (_isResponse)
            writeResponse(out);
        else
            writeRequest(out);
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        this._isResponse = in.readBoolean();
        if (this._isResponse)
            readResponse(in);
        else
            readRequest(in);
    }

    protected void writeRequest(ObjectOutput out)
            throws IOException {
        IOUtils.writeObject(out, _spaceContext);
    }

    protected void readRequest(ObjectInput in)
            throws IOException, ClassNotFoundException {
        this._spaceContext = IOUtils.readObject(in);
    }

    protected void writeResponse(ObjectOutput out)
            throws IOException {
    }

    protected void readResponse(ObjectInput in)
            throws IOException, ClassNotFoundException {
    }
}
