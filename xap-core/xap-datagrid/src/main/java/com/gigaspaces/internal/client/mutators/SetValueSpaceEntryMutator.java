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
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.server.MutableServerEntry;
import com.gigaspaces.sync.change.SetOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * @author Niv Ingberg
 * @since 9.1
 */
public final class SetValueSpaceEntryMutator extends SpaceEntryPathMutator {
    private static final long serialVersionUID = 1L;

    private Serializable _value;

    public SetValueSpaceEntryMutator() {
    }

    public SetValueSpaceEntryMutator(String path, Serializable value) {
        super(path);
        this._value = value;
    }

    @Override
    public Object change(MutableServerEntry entry) {
        entry.setPathValue(getPath(), _value);
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _value);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this._value = IOUtils.readObject(in);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("value", _value);
    }

    @Override
    public String getName() {
        return SetOperation.NAME;
    }

    public Serializable getValue() {
        return _value;
    }
}
