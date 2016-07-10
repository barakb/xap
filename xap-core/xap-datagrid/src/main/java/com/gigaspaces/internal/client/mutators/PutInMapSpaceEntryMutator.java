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
import com.gigaspaces.sync.change.PutInMapOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Map;

/**
 * @author eitany
 * @since 9.1
 */
public final class PutInMapSpaceEntryMutator
        extends SpaceEntryPathMutator {

    private static final long serialVersionUID = 1L;
    private Serializable _key;
    private Serializable _value;

    public PutInMapSpaceEntryMutator() {
    }


    public PutInMapSpaceEntryMutator(String path, Serializable key,
                                     Serializable value) {
        super(path);
        this._key = key;
        this._value = value;
    }


    @Override
    public Object change(MutableServerEntry entry) {
        Map oldValue = (Map) entry.getPathValue(getPath());
        if (oldValue == null)
            throw new IllegalStateException("No map instance exists under the given path '" + getPath() + "', in order to put a key value pair a map instance must exists");
        Map newValue = CollectionUtils.cloneMap(oldValue);
        Object previousValue = newValue.put(_key, _value);
        int size = newValue.size();
        entry.setPathValue(getPath(), newValue);
        return new MapChangeSpaceEntryMutatorResult((Serializable) previousValue, size);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _key);
        IOUtils.writeObject(out, _value);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        _key = IOUtils.readObject(in);
        _value = IOUtils.readObject(in);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("key", _key);
        textualizer.append("value", _value);
    }

    @Override
    public String getName() {
        return PutInMapOperation.NAME;
    }

    public Serializable getKey() {
        return _key;
    }

    public Serializable getValue() {
        return _value;
    }

}
