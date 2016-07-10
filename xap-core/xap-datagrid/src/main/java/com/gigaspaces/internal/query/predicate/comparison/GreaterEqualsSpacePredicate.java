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

import java.util.Comparator;

/**
 * An implementation of greater than or equals to comparison: This predicate returns true if and
 * only if the predicate's argument is greater than or equals to the expected value. Comparison is
 * performed using the compareTo method.
 *
 * @author Niv ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class GreaterEqualsSpacePredicate extends ComparableScalarSpacePredicate {
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor for Externalizable.
     */
    public GreaterEqualsSpacePredicate() {
    }

    /**
     * Creates a greater-equals predicate using the specified expected value.
     *
     * @param expectedValue Expected value.
     */
    public GreaterEqualsSpacePredicate(Comparable<?> expectedValue) {
        super(expectedValue);
    }

    /**
     * Creates a greater-equals predicate using the specified expected value and comparator.
     *
     * @param expectedValue Expected value.
     * @param comparator    Comparator to use while comparing.
     */
    public GreaterEqualsSpacePredicate(Comparable<?> expectedValue, Comparator<?> comparator) {
        super(expectedValue, comparator);
    }

    @Override
    protected boolean compare(int compareResult) {
        return compareResult <= 0;
    }

    @Override
    protected String getOperatorName() {
        return "GE";
    }
}
