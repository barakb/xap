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

package com.gigaspaces.internal.query.predicate.comparison;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.predicate.AbstractSpacePredicate;
import com.gigaspaces.internal.query.predicate.ISpacePredicate;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents an In predicate. This predicate returns true if and only if the predicate's argument
 * equals to any of the expected values.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class InSpacePredicate extends AbstractSpacePredicate {
    private static final long serialVersionUID = 1L;

    private Set _inValues;
    private transient ISpacePredicate[] _operands;

    /**
     * Default constructor for Externalizable.
     */
    public InSpacePredicate() {
    }

    /**
     * Creates an In predicate using the specified values.
     *
     * @param values Values to test.
     */
    public InSpacePredicate(Object... values) {
        _inValues = new HashSet<Object>();
        for (Object value : values) {
            _inValues.add(value);
        }
    }

    /**
     * Creates an In predicate using the specified values.
     *
     * @param inValues Values to test.
     */
    public InSpacePredicate(Set<?> inValues) {
        _inValues = inValues;
    }

    /**
     * Returns the number of expected values.
     */
    public int getNumOfExpectedValues() {
        return _inValues.size();
    }

    public Set<Object> getExpectedValues() {
        return _inValues;
    }

    @Override
    public boolean execute(Object target) {
        if (_operands == null)
            prepareOperands();
        for (ISpacePredicate operand : _operands) {
            if (operand.execute(target))
                return true;
        }
        return false;
    }

    private void prepareOperands() {
        final ISpacePredicate[] inOperands = new ISpacePredicate[_inValues.size()];
        int index = 0;
        for (Object value : _inValues) {
            inOperands[index++] = new EqualsSpacePredicate(value);
        }
        _operands = inOperands;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        _inValues = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);
        IOUtils.writeObject(out, _inValues);
    }

    public Set<?> getInValues() {
        return _inValues;
    }

}
