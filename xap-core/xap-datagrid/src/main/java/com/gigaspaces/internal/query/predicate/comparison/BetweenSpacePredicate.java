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
import com.gigaspaces.internal.utils.ObjectUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;

/**
 * Represents a Between predicate. This is equivalent to using an And Predicate with GreaterEquals
 * and LessEquals.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class BetweenSpacePredicate extends AbstractSpacePredicate {
    private static final long serialVersionUID = 1L;

    private ComparableScalarSpacePredicate _lowPredicate;
    private ComparableScalarSpacePredicate _highPredicate;

    /**
     * Default constructor for Externalizable.
     */
    public BetweenSpacePredicate() {
    }

    /**
     * Creates a Between predicate.
     *
     * @param low  Low limit for expected value.
     * @param high High limit for expected value.
     */
    public BetweenSpacePredicate(Comparable<?> low, Comparable<?> high) {
        this(low, high, null, true, true);
    }

    /**
     * Creates a Between predicate.
     *
     * @param low        Low limit for expected value.
     * @param high       High limit for expected value.
     * @param comparator Comparator to use for comparison.
     */
    public BetweenSpacePredicate(Comparable<?> low, Comparable<?> high, Comparator<?> comparator) {
        this(low, high, comparator, true, true);
    }

    /**
     * Creates a Between predicate.
     *
     * @param low           Low limit for expected value.
     * @param high          High limit for expected value.
     * @param lowInclusive  Determines if low limit comparison is inclusive.
     * @param highInclusive Determines if high limit comparison is inclusive.
     */
    public BetweenSpacePredicate(Comparable<?> low, Comparable<?> high,
                                 boolean lowInclusive, boolean highInclusive) {
        this(low, high, null, lowInclusive, highInclusive);
    }

    /**
     * Creates a Between predicate.
     *
     * @param low           Low limit for expected value.
     * @param high          High limit for expected value.
     * @param comparator    Comparator to use for comparison.
     * @param lowInclusive  Determines if low limit comparison is inclusive.
     * @param highInclusive Determines if high limit comparison is inclusive.
     */
    public BetweenSpacePredicate(Comparable<?> low, Comparable<?> high, Comparator<?> comparator,
                                 boolean lowInclusive, boolean highInclusive) {
        this._lowPredicate = lowInclusive
                ? new GreaterEqualsSpacePredicate(low, comparator)
                : new GreaterSpacePredicate(low, comparator);
        this._highPredicate = highInclusive
                ? new LessEqualsSpacePredicate(high, comparator)
                : new LessSpacePredicate(high, comparator);
    }

    /**
     * Gets the predicate which tests the low limit of the between range.
     */
    public ComparableScalarSpacePredicate getLowPredicate() {
        return _lowPredicate;
    }

    /**
     * Gets the predicate which tests the high limit of the between range.
     */
    public ComparableScalarSpacePredicate getHighPredicate() {
        return _highPredicate;
    }

    @Override
    public boolean execute(Object target) {
        return _lowPredicate.execute(target) && _highPredicate.execute(target);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        BetweenSpacePredicate other = (BetweenSpacePredicate) obj;
        if (!ObjectUtils.equals(this.getLowPredicate(), other.getLowPredicate()))
            return false;
        if (!ObjectUtils.equals(this.getHighPredicate(), other.getHighPredicate()))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        this._lowPredicate = IOUtils.readObject(in);
        this._highPredicate = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeObject(out, _lowPredicate);
        IOUtils.writeObject(out, _highPredicate);
    }
}
