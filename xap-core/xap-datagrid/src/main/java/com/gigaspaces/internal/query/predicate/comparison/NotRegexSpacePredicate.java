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

@com.gigaspaces.api.InternalApi
public class NotRegexSpacePredicate extends ScalarSpacePredicate {
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor for Externalizable.
     */
    public NotRegexSpacePredicate() {
    }

    /**
     * Creates a scalar predicate using the specified expected value
     *
     * @param pattern Expected value.
     */
    public NotRegexSpacePredicate(String pattern) {
        super(pattern, null);

        if (pattern == null)
            throw new IllegalArgumentException("Argument 'pattern' cannot be null.");

    }

    @Override
    protected boolean match(Object actual, Object expected) {
        // expected value can not be null - so any value that equals null doesn't match
        if (actual == null)
            return false;

        return !actual.toString().matches(expected.toString());
    }

    @Override
    protected String getOperatorName() {
        return "REGEX";
    }


}
