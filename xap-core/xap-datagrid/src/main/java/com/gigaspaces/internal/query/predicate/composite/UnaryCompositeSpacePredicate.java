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
 * A common basic implementation for unary composite space predicate implementations.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class UnaryCompositeSpacePredicate extends CompositeSpacePredicate {
    private static final long serialVersionUID = 1L;

    private ISpacePredicate _operand;

    /**
     * Default constructor for Externalizable.
     */
    protected UnaryCompositeSpacePredicate() {
    }

    /**
     * Creates an unary predicate using the specified operands.
     *
     * @param operand Operand for unary predicate.
     */
    protected UnaryCompositeSpacePredicate(ISpacePredicate operand) {
        if (operand == null)
            throw new IllegalArgumentException("Argument 'operand' cannot be null.");

        this._operand = operand;
    }

    /**
     * Gets the operand of this unary predicate.
     *
     * @return the operand of this unary predicate.
     */
    public ISpacePredicate getOperand() {
        return _operand;
    }

    @Override
    public boolean execute(Object target) {
        return execute(target, _operand);
    }

    /**
     * Executes the operand predicate on the target.
     *
     * @param target  Target object for predicate execution.
     * @param operand Predicate to execute.
     */
    protected abstract boolean execute(Object target, ISpacePredicate operand);

    @Override
    public String toString() {
        return getOperatorName() + "(" + _operand.toString() + ")";
    }

    /**
     * Gets this operator's name, to be used in toString().
     *
     * @return This operator's name.
     */
    protected abstract String getOperatorName();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        UnaryCompositeSpacePredicate other = (UnaryCompositeSpacePredicate) obj;

        if (!ObjectUtils.equals(this.getOperand(), other.getOperand()))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        this._operand = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeObject(out, _operand);
    }
}
