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

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.internal.space.responses.AddTypeIndexesResponseInfo;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class AddTypeIndexesRequestInfo extends AbstractSpaceRequestInfo implements ISwapExternalizable {
    private static final long serialVersionUID = 1L;

    private String _typeName;
    private SpaceIndex[] _indexes;
    private transient AsyncFutureListener<AddTypeIndexesResult> _listener;


    /**
     * Required for Externalizable
     */
    public AddTypeIndexesRequestInfo() {
    }

    public AddTypeIndexesRequestInfo(String typeName, SpaceIndex[] indexes, AsyncFutureListener<AddTypeIndexesResult> listener) {
        this._typeName = typeName;
        this._indexes = indexes;
        this._listener = listener;
    }

    public String getTypeName() {
        return _typeName;
    }

    public SpaceIndex[] getIndexes() {
        return _indexes;
    }

    public AsyncFutureListener<AddTypeIndexesResult> getListener() {
        return _listener;
    }

    public void setListener(AsyncFutureListener<AddTypeIndexesResult> listener) {
        _listener = listener;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        // Write type name:
        IOUtils.writeRepetitiveString(out, this._typeName);
        // Write indexes array:
        if (_indexes == null)
            out.writeInt(-1);
        else {
            out.writeInt(_indexes.length);
            for (int i = 0; i < _indexes.length; i++)
                IOUtils.writeObject(out, _indexes[i]);
        }
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);

        // Write type name:
        IOUtils.writeRepetitiveString(out, this._typeName);
        // Write indexes array:
        if (_indexes == null)
            out.writeInt(-1);
        else {
            out.writeInt(_indexes.length);
            for (int i = 0; i < _indexes.length; i++) {
                ISpaceIndex spaceIndex = (ISpaceIndex) _indexes[i];
                IOUtils.writeSwapExternalizableObject(out, spaceIndex);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        // Read type name:
        this._typeName = IOUtils.readRepetitiveObject(in);
        // Read indexes array:
        int numOfIndexes = in.readInt();
        if (numOfIndexes >= 0) {
            _indexes = new ISpaceIndex[numOfIndexes];
            for (int i = 0; i < numOfIndexes; i++)
                _indexes[i] = IOUtils.readObject(in);
        }
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        // Read type name:
        this._typeName = IOUtils.readRepetitiveObject(in);
        // Read indexes array:
        int numOfIndexes = in.readInt();
        if (numOfIndexes >= 0) {
            _indexes = new ISpaceIndex[numOfIndexes];
            for (int i = 0; i < numOfIndexes; i++) {
                ISpaceIndex index = IOUtils.readSwapExternalizableObject(in);
                _indexes[i] = index;
            }
        }

    }

    public AddTypeIndexesResult reduce(List<AsyncResult<AddTypeIndexesResult>> results)
            throws Exception {
        AddTypeIndexesResult finalResult = null;

        for (AsyncResult<AddTypeIndexesResult> result : results) {
            if (result.getException() != null)
                throw result.getException();

            ITypeDesc[] updatedTypeDescriptors = ((AddTypeIndexesResponseInfo) result.getResult()).getUpdatedTypeDescriptors();
            if (finalResult == null || (updatedTypeDescriptors != null && updatedTypeDescriptors.length > 0)) {
                finalResult = result.getResult();
            }
        }

        return finalResult;
    }
}
