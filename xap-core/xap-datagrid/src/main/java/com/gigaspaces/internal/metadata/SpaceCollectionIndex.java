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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryCollectionValuesExtractor;
import com.gigaspaces.metadata.index.AbstractSpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * An index for a path containing a collection ("[*]").
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceCollectionIndex extends AbstractSpaceIndex {
    private static final long serialVersionUID = -7040439531937379040L;

    public static final String COLLECTION_INDICATOR = "[*]";
    protected SpaceEntryCollectionValuesExtractor _valuesExtractor;

    public SpaceCollectionIndex() {
    }

    public SpaceCollectionIndex(String path, SpaceIndexType indexType, boolean unique) {
        super(path, indexType, unique);
        _valuesExtractor = new SpaceEntryCollectionValuesExtractor(path);
    }

    public Object getIndexValue(ServerEntry entry) {
        return _valuesExtractor.getValue(entry);
    }

    public Object getIndexValueForTemplate(ServerEntry entry) {
        return getIndexValueFromCustomIndex(entry);
    }

    @Override
    public boolean isMultiValuePerEntryIndex() {
        return true;
    }

    @Override
    public MultiValuePerEntryIndexTypes getMultiValueIndexType() {
        return MultiValuePerEntryIndexTypes.COLLECTION;
    }

    @Override
    public IndexOriginTypes getIndexOriginType() {
        return IndexOriginTypes.COLLECTION;
    }

    @Override
    protected void readExternalImpl(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        _valuesExtractor = IOUtils.readObject(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _valuesExtractor = IOUtils.readNullableSwapExternalizableObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out) throws IOException {
        super.writeExternalImpl(out);
        IOUtils.writeObject(out, _valuesExtractor);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeNullableSwapExternalizableObject(out, _valuesExtractor);
    }


}
