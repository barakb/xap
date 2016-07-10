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

package com.gigaspaces.metadata.index;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This type is intended for internal usage and should not be used directly. Use {@link
 * SpaceIndexFactory} to create space indexes.
 *
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpacePathIndex extends AbstractSpaceIndex {
    private static final long serialVersionUID = 1L;

    private SpaceEntryPathGetter _indexValueGetter;

    public SpacePathIndex() {
    }

    public SpacePathIndex(String path, SpaceIndexType indexType, boolean unique) {
        super(path, indexType, unique);
        _indexValueGetter = new SpaceEntryPathGetter(path);
    }

    public Object getIndexValue(ServerEntry entry) {
        return _indexValueGetter.getValue(entry);
    }

    /**
     * @return the  index origin
     */
    @Override
    public IndexOriginTypes getIndexOriginType() {
        return IndexOriginTypes.PATH;
    }

    public Object getIndexValueForTemplate(ServerEntry entry) {
        return getIndexValueFromCustomIndex(entry);
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        _indexValueGetter = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);
        IOUtils.writeObject(out, _indexValueGetter);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeNullableSwapExternalizableObject(out, _indexValueGetter);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _indexValueGetter = IOUtils.readNullableSwapExternalizableObject(in);
    }
}
