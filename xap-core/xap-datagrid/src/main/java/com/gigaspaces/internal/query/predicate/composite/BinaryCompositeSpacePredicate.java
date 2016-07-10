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

package com.gigaspaces.internal.query.predicate.composite;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.predicate.ISpacePredicate;
import com.gigaspaces.internal.utils.ObjectUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A common basic implementation for binary composite space predicate implementations.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class BinaryCompositeSpacePredicate extends CompositeSpacePredicate {
    private static final long serialVersionUID = 1L;

    private ISpacePredicate _leftOperand;
    private ISpacePredicate _rightOperand;

    /**
     * Default constructor for Externalizable.
     */
    protected BinaryCompositeSpacePredicate() {
    }

    /**
     * Creates a binary predicate using the specified operands.
     *
     * @param leftOperand  Left operand of the binary predicate.
     * @param rightOperand Right operand of the binary predicate.
     */
    protected BinaryCompositeSpacePredicate(ISpacePredicate leftOperand, ISpacePredicate rightOperand) {
        if (leftOperand == null)
            throw new IllegalArgumentException("Argument 'leftOperand' cannot be null.");
        if (rightOperand == null)
            throw new IllegalArgumentException("Argument 'rightOperand' cannot be null.");
        this._leftOperand = leftOperand;
        this._rightOperand = rightOperand;
    }

    /**
     * Gets the left operand of this binary predicate.
     *
     * @return The left operand.
     */
    public ISpacePredicate getLeftOperand() {
        return _leftOperand;
    }

    /**
     * Gets the right operand of this binary predicate.
     *
     * @return The right operand.
     */
    public ISpacePredicate getRightOperand() {
        return _rightOperand;
    }

    @Override
    public boolean execute(Object target) {
        return execute(target, _leftOperand, _rightOperand);
    }

    protected abstract boolean execute(Object target,
                                       ISpacePredicate leftOperand, ISpacePredicate rightOperand);

    @Override
    public String toString() {
        return "(" + _leftOperand + ") " + getOperatorName() + " (" + _rightOperand + ")";
    }

    protected abstract String getOperatorName();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        BinaryCompositeSpacePredicate other = (BinaryCompositeSpacePredicate) obj;

        if (!ObjectUtils.equals(this.getLeftOperand(), other.getLeftOperand()))
            return false;
        if (!ObjectUtils.equals(this.getRightOperand(), other.getRightOperand()))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        this._leftOperand = IOUtils.readObject(in);
        this._rightOperand = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeObject(out, _leftOperand);
        IOUtils.writeObject(out, _rightOperand);
    }
}
