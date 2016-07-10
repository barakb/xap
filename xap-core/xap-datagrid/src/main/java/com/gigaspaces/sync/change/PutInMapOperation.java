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
import com.gigaspaces.internal.client.mutators.MapChangeSpaceEntryMutatorResult;
import com.gigaspaces.internal.client.mutators.PutInMapSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.SpaceEntryPathMutator;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a change operation which correlate with a {@link ChangeSet#putInMap(String,
 * java.io.Serializable, java.io.Serializable)} invocation.
 *
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class PutInMapOperation {
    /**
     * The name of the operation.
     */
    public static final String NAME = "putInMap";

    private PutInMapOperation() {
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
     * @return the key that was put.
     */
    public static Serializable getKey(ChangeOperation changeOperation) {
        if (!(changeOperation instanceof PutInMapSpaceEntryMutator))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperation);
        return ((PutInMapSpaceEntryMutator) changeOperation).getKey();
    }

    /**
     * @return the value that was put with for the given key.
     */
    public static Serializable getValue(ChangeOperation changeOperation) {
        if (!(changeOperation instanceof PutInMapSpaceEntryMutator))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperation);
        return ((PutInMapSpaceEntryMutator) changeOperation).getValue();
    }

    /**
     * @return the return value of the {@link Map#put(Object, Object)} operation that was applied.
     * @since 9.7
     */
    public static Serializable getPreviousValue(ChangeOperationResult changeOperationResult) {
        if (!represents(changeOperationResult.getOperation()))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperationResult);
        return ((MapChangeSpaceEntryMutatorResult) changeOperationResult.getResult()).getValue();
    }

    /**
     * @return the size of the map after the {@link Map#put(Object, Object)} operation that was
     * applied.
     * @since 9.7
     */
    public static int getNewSize(ChangeOperationResult changeOperationResult) {
        if (!represents(changeOperationResult.getOperation()))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperationResult);
        return ((MapChangeSpaceEntryMutatorResult) changeOperationResult.getResult()).getSize();
    }
}
