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

package com.gigaspaces.internal.space.responses;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class AddTypeIndexesResponseInfo extends AbstractSpaceResponseInfo implements AddTypeIndexesResult {
    private static final long serialVersionUID = 1L;

    private SpaceMetadataException _metadataException;
    private ITypeDesc[] _updatedTypeDescriptors;

    /**
     * Required for Externalizable
     */
    public AddTypeIndexesResponseInfo() {
    }

    public SpaceMetadataException getMetadataException() {
        return _metadataException;
    }

    public void setMetadataException(SpaceMetadataException metadataException) {
        this._metadataException = metadataException;
    }

    public ITypeDesc[] getUpdatedTypeDescriptors() {
        return _updatedTypeDescriptors;
    }

    public void setUpdatedTypeDescriptors(ITypeDesc[] updatedTypeDescriptors) {
        this._updatedTypeDescriptors = updatedTypeDescriptors;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        // Write exception (if any):
        IOUtils.writeObject(out, _metadataException);

        // Write updated type descriptors:
        if (_updatedTypeDescriptors == null)
            out.writeInt(-1);
        else {
            out.writeInt(_updatedTypeDescriptors.length);
            for (int i = 0; i < _updatedTypeDescriptors.length; i++)
                IOUtils.writeObject(out, _updatedTypeDescriptors[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        // Read exception (if any):
        _metadataException = IOUtils.readObject(in);

        // Read updated type descriptors:
        int length = in.readInt();
        if (length != -1) {
            _updatedTypeDescriptors = new ITypeDesc[length];
            for (int i = 0; i < length; i++)
                _updatedTypeDescriptors[i] = IOUtils.readObject(in);
        }
    }
}
