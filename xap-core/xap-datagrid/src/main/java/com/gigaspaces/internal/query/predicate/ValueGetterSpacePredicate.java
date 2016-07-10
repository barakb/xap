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

package com.gigaspaces.internal.query.predicate;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.valuegetter.ISpaceValueGetter;
import com.gigaspaces.internal.utils.ObjectUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class ValueGetterSpacePredicate<T> extends AbstractSpacePredicate {
    private static final long serialVersionUID = 1L;

    private ISpaceValueGetter<T> _valueGetter;
    private ISpacePredicate _predicate;

    public ValueGetterSpacePredicate() {
    }

    public ValueGetterSpacePredicate(ISpaceValueGetter<T> valueGetter, ISpacePredicate predicate) {
        this._valueGetter = valueGetter;
        this._predicate = predicate;
    }

    public ISpaceValueGetter<?> getValueGetter() {
        return _valueGetter;
    }

    public ISpacePredicate getPredicate() {
        return _predicate;
    }

    @Override
    public boolean execute(Object target) {
        Object value = _valueGetter.getValue((T) target);
        return _predicate.execute(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        ValueGetterSpacePredicate<T> other = (ValueGetterSpacePredicate<T>) obj;
        if (!ObjectUtils.equals(this.getValueGetter(), other.getValueGetter()))
            return false;
        if (!ObjectUtils.equals(this.getPredicate(), other.getPredicate()))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        _valueGetter = IOUtils.readObject(in);
        _predicate = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeObject(out, _valueGetter);
        IOUtils.writeObject(out, _predicate);
    }
}
