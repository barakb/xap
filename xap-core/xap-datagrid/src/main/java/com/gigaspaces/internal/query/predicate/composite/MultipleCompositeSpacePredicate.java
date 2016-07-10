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
import com.gigaspaces.internal.utils.CollectionUtils;
import com.j_spaces.core.cache.CacheManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public abstract class MultipleCompositeSpacePredicate extends CompositeSpacePredicate {
    private static final long serialVersionUID = 1L;

    private List<ISpacePredicate> _operandsList;
    private transient ISpacePredicate[] _operandsArray;
    protected transient CacheManager _cacheManager;

    /**
     * Default constructor for Externalizable.
     */
    public MultipleCompositeSpacePredicate() {
    }

    /**
     * Creates a multiple composite space predicate using the specified operands.
     */
    public MultipleCompositeSpacePredicate(List<ISpacePredicate> operands) {
        if (operands == null)
            throw new IllegalArgumentException("Argument 'operands' cannot be null.");
        for (int i = 0; i < operands.size(); i++)
            if (operands.get(i) == null)
                throw new IllegalArgumentException("Argument 'operands[" + i + "]' cannot be null.");

        this._operandsList = operands;
    }

    /**
     * Creates a multiple composite space predicate using the specified operands.
     */
    public MultipleCompositeSpacePredicate(ISpacePredicate... operands) {
        if (operands == null)
            throw new IllegalArgumentException("Argument 'operands' cannot be null.");

        this._operandsList = new ArrayList<ISpacePredicate>(operands.length);

        for (int i = 0; i < operands.length; i++) {
            if (operands[i] == null)
                throw new IllegalArgumentException("Argument 'operands[" + i + "]' cannot be null.");
            this._operandsList.add(operands[i]);
        }
    }

    /**
     * Gets the number of operands in this composite predicate.
     */
    public int getNumOfOperands() {
        return _operandsList.size();
    }

    /**
     * Gets the operand at the specified index.
     *
     * @param index Index of operand to get.
     * @return Operand at specified index.
     */
    public ISpacePredicate getOperand(int index) {
        return _operandsList.get(index);
    }

    /**
     * Sets the operand at the specified index.
     *
     * @param index   Index of operand to set.
     * @param operand new operand.
     */
    public void setOperand(int index, ISpacePredicate operand) {
        _operandsList.set(index, operand);
    }

    /**
     * Adds an operand at the end of the current operands list.
     *
     * @param operand Operand to add.
     */
    public void addOperand(ISpacePredicate operand) {
        if (_operandsList == null)
            _operandsList = new ArrayList<ISpacePredicate>();
        _operandsList.add(operand);
    }

    @Override
    public boolean execute(Object target) {
        if (_operandsArray == null)
            _operandsArray = prepare(_operandsList);
        return execute(target, _operandsArray);
    }

    private static ISpacePredicate[] prepare(List<ISpacePredicate> list) {
        final int length = (list == null) ? 0 : list.size();

        ISpacePredicate[] array = new ISpacePredicate[length];
        for (int i = 0; i < length; i++)
            array[i] = list.get(i);

        return array;
    }

    protected abstract boolean execute(Object target, ISpacePredicate[] operands);

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        MultipleCompositeSpacePredicate other = (MultipleCompositeSpacePredicate) obj;

        if (!CollectionUtils.equals(this._operandsList, other._operandsList))
            return false;

        return true;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        final int length = in.readInt();
        this._operandsList = new ArrayList<ISpacePredicate>(length);
        for (int i = 0; i < length; i++)
            this._operandsList.add((ISpacePredicate) IOUtils.readObject(in));
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        final int length = (_operandsList == null) ? 0 : _operandsList.size();
        out.writeInt(length);
        for (int i = 0; i < length; i++)
            IOUtils.writeObject(out, _operandsList.get(i));
    }

    @Override
    public boolean requiresCacheManagerForExecution() {
        return true;
    }

    @Override
    public void setCacheManagerForExecution(CacheManager cacheManager) {
        this._cacheManager = cacheManager;
    }
}
