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

package com.gigaspaces.sync.change;

import com.gigaspaces.client.ChangeOperationResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.internal.client.mutators.IncrementSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.SpaceEntryPathMutator;

/**
 * Represents a change operation which correlate with a {@link ChangeSet#increment(String, int)} or
 * {@link ChangeSet#decrement(String, int)} invocation.
 *
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class IncrementOperation {
    /**
     * The name of the operation.
     */
    public static final String NAME = "increment";

    private IncrementOperation() {
    }

    /**
     * @return true if this operation type represents the given change operation.
     */
    public static boolean represents(ChangeOperation changeOperation) {
        return NAME.equals(changeOperation.getName());
    }

    /**
     * @return the path the operation changed.
     */
    public static String getPath(ChangeOperation changeOperation) {
        if (!(changeOperation instanceof SpaceEntryPathMutator))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperation);
        return ((SpaceEntryPathMutator) changeOperation).getPath();
    }

    /**
     * @return the delta that was incremented (negative for decrement operation).
     */
    public static Number getDelta(ChangeOperation changeOperation) {
        if (!(changeOperation instanceof IncrementSpaceEntryMutator))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperation);
        return ((IncrementSpaceEntryMutator) changeOperation).getDelta();
    }

    /**
     * @return the new value of the property after the increment operation was applied
     * @since 9.7
     */
    public static Number getNewValue(ChangeOperationResult changeOperationResult) {
        if (!represents(changeOperationResult.getOperation()))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperationResult);
        return (Number) changeOperationResult.getResult();
    }
}
