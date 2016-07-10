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

package com.gigaspaces.internal.client.mutators;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.server.MutableServerEntry;
import com.gigaspaces.sync.change.AddAllToCollectionOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;

/**
 * @author eitany
 * @since 9.1
 */
public final class AddAllToCollectionSpaceEntryMutator
        extends SpaceEntryPathMutator {

    private static final long serialVersionUID = 1L;
    private Collection<? extends Serializable> _items;

    public AddAllToCollectionSpaceEntryMutator() {
    }

    public AddAllToCollectionSpaceEntryMutator(String path,
                                               Collection<? extends Serializable> items) {
        super(path);
        _items = items;
    }

    @Override
    public CollectionChangeSpaceEntryMutatorResult change(MutableServerEntry entry) {
        Collection oldValue = (Collection) entry.getPathValue(getPath());
        if (oldValue == null)
            throw new IllegalStateException("No collection instance exists under the given path '" + getPath() + "', in order to add items a collection instance must exists");
        Collection newValue = CollectionUtils.cloneCollection(oldValue);
        boolean changed = newValue.addAll(_items);
        int size = newValue.size();
        entry.setPathValue(getPath(), newValue);
        return new CollectionChangeSpaceEntryMutatorResult(changed, size);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _items);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _items = IOUtils.readObject(in);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.appendIterable("items", _items);
    }

    @Override
    public String getName() {
        return AddAllToCollectionOperation.NAME;
    }

    public Collection<? extends Serializable> getItems() {
        return _items;
    }

}
