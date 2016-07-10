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
import com.gigaspaces.internal.query.valuegetter.ISpaceValueGetter;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Default implementation of the space custom index meta data
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class CustomIndex extends AbstractSpaceIndex {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    protected ISpaceValueGetter<ServerEntry> _indexValueGetter;

    public CustomIndex() {
    }

    public CustomIndex(String indexName,
                       ISpaceValueGetter<ServerEntry> indexValueGetter, boolean isUnique, SpaceIndexType indexType) {
        super(indexName, indexType, isUnique);
        _indexValueGetter = indexValueGetter;
    }

    public Object getIndexValue(ServerEntry entry) {
        return _indexValueGetter.getValue(entry);
    }

    /**
     * @return the  index origin
     */
    @Override
    public IndexOriginTypes getIndexOriginType() {
        return IndexOriginTypes.CUSTOM;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        _indexValueGetter = IOUtils.readObject(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _indexValueGetter = IOUtils.readNullableSwapExternalizableObject(in);
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

    public Object getIndexValueForTemplate(ServerEntry entry) {
        return getIndexValueFromCustomIndex(entry);
    }
}
