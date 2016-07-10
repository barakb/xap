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

import java.util.List;

/**
 * An implementation of a logical multiple AND condition: This predicate returns true if and only if
 * all operands return true.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class AllSpacePredicate extends MultipleCompositeSpacePredicate {
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor for Externalizable.
     */
    public AllSpacePredicate() {
    }

    /**
     * Creates a multiple composite space predicate using the specified operands.
     */
    public AllSpacePredicate(List<ISpacePredicate> operands) {
        super(operands);
    }

    /**
     * Creates a multiple composite space predicate using the specified operands.
     */
    public AllSpacePredicate(ISpacePredicate... operands) {
        super(operands);
    }

    @Override
    protected boolean execute(Object target, ISpacePredicate[] operands) {
        final int length = operands.length;

        for (int i = 0; i < length; i++) {
            if (operands[i].requiresCacheManagerForExecution()) {
                operands[i].setCacheManagerForExecution(_cacheManager);
            }
            if (!operands[i].execute(target)) {
                return false;
            }
        }
        return true;
    }
}
