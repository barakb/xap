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
import com.gigaspaces.internal.utils.ObjectUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;

/**
 * A common basic implementation for comparable scalar comparison predicate implementations.
 *
 * @author Niv ingberg
 * @since 7.1
 */
public abstract class ComparableScalarSpacePredicate extends ScalarSpacePredicate {
    private static final long serialVersionUID = -6819749107818741458L;

    private Comparator _comparator;
    private transient Comparable _preparedExpectedValue;

    /**
     * Default constructor for Externalizable.
     */
    protected ComparableScalarSpacePredicate() {
    }

    /**
     * Creates a comparable scalar predicate using the specified expected value.
     *
     * @param expectedValue Expected value.
     */
    protected ComparableScalarSpacePredicate(Comparable<?> expectedValue) {
        this(expectedValue, null);
    }

    /**
     * Creates a comparable scalar predicate using the specified expected value and comparator.
     *
     * @param expectedValue Expected value.
     * @param comparator    Comparator to use while comparing.
     */
    protected ComparableScalarSpacePredicate(Comparable<?> expectedValue, Comparator<?> comparator) {
        super(expectedValue, null);
        if (expectedValue == null)
            throw new IllegalArgumentException("Argument 'expectedValue' cannot be null.");

        this._comparator = comparator;
    }

    /**
     * Gets the comparator associated with this predicate.
     *
     * @return The comparator associated with this predicate.
     */
    public Comparator getComparator() {
        return _comparator;
    }

    @Override
    protected boolean match(Object actual, Object expected) {
        if (actual == null)
            return false;

        if (_preparedExpectedValue == null)
            _preparedExpectedValue = prepare(expected);

        int result;
        if (_comparator == null)
            result = _preparedExpectedValue.compareTo(actual);
        else
            result = _comparator.compare(_preparedExpectedValue, actual);

        return compare(result);
    }

    private Comparable<?> prepare(Object expected) {
        return (Comparable<?>) expected;
    }

    protected abstract boolean compare(int compareResult);

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        ComparableScalarSpacePredicate other = (ComparableScalarSpacePredicate) obj;

        if (!ObjectUtils.equals(this.getComparator(), other.getComparator()))
            return false;

        return super.equals(obj);
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        _comparator = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeObject(out, _comparator);
    }
}
