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
import com.gigaspaces.internal.query.ConvertedObjectWrapper;
import com.gigaspaces.internal.query.predicate.AbstractSpacePredicate;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A common basic implementation for scalar comparison predicate implementations.
 *
 * @author Niv ingberg
 * @since 7.1
 */
public abstract class ScalarSpacePredicate extends AbstractSpacePredicate {
    private static final long serialVersionUID = -2962394684649863195L;

    protected Object _expectedValue;
    protected FunctionCallDescription functionCallDescription;
    protected transient ConvertedObjectWrapper _convertedValueWrapper;

    /**
     * Default constructor for Externalizable.
     */
    protected ScalarSpacePredicate() {
    }

    /**
     * Creates a scalar predicate using the specified expected value
     *
     * @param expectedValue Expected value.
     */
    protected ScalarSpacePredicate(Object expectedValue, FunctionCallDescription functionCallDescription) {
        this._expectedValue = expectedValue;
        this.functionCallDescription = functionCallDescription;
    }

    /**
     * Gets the expected value.
     *
     * @return Expected value.
     */
    public Object getExpectedValue() {
        return this._expectedValue;
    }

    protected Object getPreparedExpectedValue(Object target) {
        // If target is null we cannot prepare - return unprepared value without caching result:
        if (target == null)
            return _expectedValue;

        // If converted value wrapper is not initialized, try to convert:
        if (_convertedValueWrapper == null)
            _convertedValueWrapper = ConvertedObjectWrapper.create(_expectedValue, target.getClass());
        // If conversion could not be performed, return null
        if (_convertedValueWrapper == null)
            return _expectedValue;
        return _convertedValueWrapper.getValue();
    }

    @Override
    public boolean execute(Object target) {
        return match(target, getPreparedExpectedValue(target));
    }

    protected abstract boolean match(Object actual, Object expected);

    @Override
    public String toString() {
        return getOperatorName() + "(" + getExpectedValue() + ")";
    }

    protected abstract String getOperatorName();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        ScalarSpacePredicate other = (ScalarSpacePredicate) obj;

        if (!ObjectUtils.equals(this.getExpectedValue(), other.getExpectedValue()))
            return false;

        if (this.functionCallDescription != null) {
            return this.functionCallDescription.equals(other.functionCallDescription);
        }
        return other.functionCallDescription == null;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        _expectedValue = IOUtils.readObject(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
            functionCallDescription = IOUtils.readObject(in);
        }
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeObject(out, _expectedValue);
        IOUtils.writeObject(out, functionCallDescription);
    }
}
