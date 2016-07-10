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

package com.gigaspaces.internal.query.valuegetter;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.AbstractTypeIntrospector;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A space value getter for getting a path from a space entry.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceEntryPathGetter extends AbstractSpaceValueGetter<ServerEntry> {
    private static final long serialVersionUID = -9025377890997556363L;

    private String _path;
    private transient String[] _tokens;

    /**
     * Default constructor required by Externalizable.
     */
    public SpaceEntryPathGetter() {
    }

    /**
     * Create a path getter using the specified path.
     */
    public SpaceEntryPathGetter(String path) {
        this._path = path;
    }

    @Override
    public Object getValue(ServerEntry target) {
        if (_tokens == null)
            prepare(target.getSpaceTypeDescriptor());

        // TODO: Validations.
        Object root = target.getPropertyValue(_tokens[0]);

        return AbstractTypeIntrospector.getPathValue(root, _tokens, _path);
    }

    public String getPath() {
        return _path;
    }

    private void prepare(SpaceTypeDescriptor typeDescriptor) {
        this._tokens = _path.split("\\.");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        SpaceEntryPathGetter other = (SpaceEntryPathGetter) obj;
        if (!ObjectUtils.equals(this._path, other._path))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        this._path = IOUtils.readString(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeString(out, _path);
    }


    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        readExternalImpl(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }
}
