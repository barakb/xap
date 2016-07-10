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
import com.gigaspaces.internal.client.mutators.CollectionChangeSpaceEntryMutatorResult;
import com.gigaspaces.internal.client.mutators.RemoveFromCollectionSpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.SpaceEntryPathMutator;

import java.io.Serializable;
import java.util.Collection;

/**
 * Represents a change operation which correlate with a {@link ChangeSet#removeFromCollection(String,
 * java.io.Serializable)} invocation.
 *
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class RemoveFromCollectionOperation {
    /**
     * The name of the operation.
     */
    public static final String NAME = "removeFromCollection";

    private RemoveFromCollectionOperation() {
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

    public static Serializable getItem(ChangeOperation changeOperation) {
        if (!(changeOperation instanceof SpaceEntryPathMutator))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperation);
        return ((RemoveFromCollectionSpaceEntryMutator) changeOperation).getItem();
    }

    /**
     * @return the return value of the {@link Collection#remove(Object)} operation that was applied.
     * @since 9.7
     */
    public static boolean isChanged(ChangeOperationResult changeOperationResult) {
        if (!represents(changeOperationResult.getOperation()))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperationResult);
        return ((CollectionChangeSpaceEntryMutatorResult) changeOperationResult.getResult()).isCollectionChanged();
    }

    /**
     * @return the size of the collection after the {@link Collection#remove(Object)} operation that
     * was applied.
     * @since 9.7
     */
    public static int getNewSize(ChangeOperationResult changeOperationResult) {
        if (!represents(changeOperationResult.getOperation()))
            throw new IllegalArgumentException("The argument is not of legit type - " + changeOperationResult);
        return ((CollectionChangeSpaceEntryMutatorResult) changeOperationResult.getResult()).getSize();
    }
}
