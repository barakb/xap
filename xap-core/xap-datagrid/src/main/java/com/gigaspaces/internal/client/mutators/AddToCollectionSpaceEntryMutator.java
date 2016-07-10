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
import com.gigaspaces.sync.change.AddToCollectionOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 9.1
 */
public final class AddToCollectionSpaceEntryMutator extends SpaceEntryPathMutator {
    private static final long serialVersionUID = 1L;

    private Serializable _item;

    public AddToCollectionSpaceEntryMutator() {
    }

    public AddToCollectionSpaceEntryMutator(String path, Serializable item) {
        super(path);
        this._item = item;
    }

    @Override
    public CollectionChangeSpaceEntryMutatorResult change(MutableServerEntry entry) {
        Collection oldValue = (Collection) entry.getPathValue(getPath());
        if (oldValue == null)
            throw new IllegalStateException("No collection instance exists under the given path '" + getPath() + "', in order to add a value a collection instance must exists");
        Collection newValue = CollectionUtils.cloneCollection(oldValue);
        boolean changed = newValue.add(_item);
        int size = newValue.size();
        entry.setPathValue(getPath(), newValue);
        return new CollectionChangeSpaceEntryMutatorResult(changed, size);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _item);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this._item = IOUtils.readObject(in);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("item", _item);
    }

    @Override
    public String getName() {
        return AddToCollectionOperation.NAME;
    }

    public Serializable getItem() {
        return _item;
    }
}
