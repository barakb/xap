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

import com.gigaspaces.internal.query.predicate.ISpacePredicate;

/**
 * An implementation of a logical binary AND condition: This predicate returns true if and only if
 * both left and right operands return true.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class AndSpacePredicate extends BinaryCompositeSpacePredicate {
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor for Externalizable.
     */
    public AndSpacePredicate() {
    }

    /**
     * Creates a binary predicate using the specified predicates.
     *
     * @param leftPredicate  Left predicate of the binary predicate.
     * @param rightPredicate Right predicate of the binary predicate.
     */
    public AndSpacePredicate(ISpacePredicate leftPredicate, ISpacePredicate rightPredicate) {
        super(leftPredicate, rightPredicate);
    }

    @Override
    protected boolean execute(Object target, ISpacePredicate leftOperand,
                              ISpacePredicate rightOperand) {
        return leftOperand.execute(target) && rightOperand.execute(target);
    }

    @Override
    protected String getOperatorName() {
        return "AND";
    }
}
