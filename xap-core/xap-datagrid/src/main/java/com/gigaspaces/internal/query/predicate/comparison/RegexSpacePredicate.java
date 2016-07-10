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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@com.gigaspaces.api.InternalApi
public class RegexSpacePredicate extends ScalarSpacePredicate {
    private static final long serialVersionUID = 1L;
    private transient Pattern pattern;

    /**
     * Default constructor for Externalizable.
     */
    public RegexSpacePredicate() {
    }

    /**
     * Creates a scalar predicate using the specified expected value
     *
     * @param expectedValue Expected value.
     */
    public RegexSpacePredicate(String expectedValue) {
        super(expectedValue, null);

        if (expectedValue == null)
            throw new IllegalArgumentException("Argument 'expectedValue' cannot be null.");
        init();

    }

    private void init() {
        pattern = Pattern.compile((String) _expectedValue);
    }

    @Override
    protected boolean match(Object actual, Object expected) {
        // expected value can not be null - so any value that equals null doesn't match
        if (actual == null)
            return false;

        Matcher m = pattern.matcher(actual.toString());
        return m.matches();
    }

    @Override
    protected String getOperatorName() {
        return "REGEX";
    }

    @Override
    protected void readExternalImpl(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        init();


    }

    @Override
    protected void writeExternalImpl(ObjectOutput out) throws IOException {
        super.writeExternalImpl(out);
    }
}
