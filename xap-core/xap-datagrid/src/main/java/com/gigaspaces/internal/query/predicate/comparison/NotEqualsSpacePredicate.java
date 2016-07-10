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

/**
 * An implementation of not equals condition: This predicate returns true if and only if the
 * predicate's argument is not equal to the expected value. Equality is performed using the object's
 * equals method. Null values are unmatched
 *
 * @author Niv ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class NotEqualsSpacePredicate extends ScalarSpacePredicate {
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor for Externalizable.
     */
    public NotEqualsSpacePredicate() {
    }

    /**
     * Creates a scalar predicate using the specified expected value
     *
     * @param expectedValue Expected value.
     */
    public NotEqualsSpacePredicate(Object expectedValue) {
        super(expectedValue, null);
    }

    @Override
    protected boolean match(Object actual, Object expected) {
        if (actual == null) {
            // Intentional not checking if expected is null for consistency with Hibernate - see http://www.javabeat.net/2008/07/null-and-not-null-comparison-in-the-hibernate-api/
            return false;
        }

        return !actual.equals(expected);
    }

    @Override
    protected String getOperatorName() {
        return "NEQ";
    }
}
