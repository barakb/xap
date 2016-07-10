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

package org.openspaces.test.extensions;

import com.google.common.collect.Iterables;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeOperationResult;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.ChangeSetInternalUtils;
import com.gigaspaces.client.ChangedEntryDetails;
import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.mutators.IncrementSpaceEntryMutator;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.sync.change.ChangeOperation;
import com.gigaspaces.sync.change.IncrementOperation;
import com.j_spaces.core.client.Modifiers;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.openspaces.core.ChangeException;
import org.openspaces.core.GigaSpace;
import org.openspaces.extensions.ChangeExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChangeExtensionTests {
    @SpaceClass
    public static class ChangeExtensionPojo {
    }

    @Test
    public void testAddAndGet() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        int delta = 2;
        int result = 3;
        String path = "path";
        IdQuery<ChangeExtensionPojo> query = new IdQuery<ChangeExtensionPojo>(ChangeExtensionPojo.class, "id");

        ChangeResult changeResult = mock(ChangeResult.class);
        when(changeResult.getNumberOfChangedEntries()).thenReturn(1);
        Collection<ChangedEntryDetails<ChangeExtensionPojo>> changeEntriesResult = new ArrayList<ChangedEntryDetails<ChangeExtensionPojo>>();
        ChangedEntryDetails<ChangeExtensionPojo> changedEntryResult = mock(ChangedEntryDetails.class);
        List<ChangeOperationResult> changeOperationsResults = new ArrayList<ChangeOperationResult>();
        ChangeOperationResult incrementChangeOperation = createIncrementChangeOperationResult(delta,
                path,
                result);
        changeOperationsResults.add(incrementChangeOperation);
        when(changedEntryResult.getChangeOperationsResults()).thenReturn(changeOperationsResults);
        changeEntriesResult.add(changedEntryResult);
        when(changeResult.getResults()).thenReturn(changeEntriesResult);
        when(gigaSpace.change(any(ISpaceQuery.class), any(ChangeSet.class), any(ChangeModifiers.class), anyLong())).thenReturn(changeResult);

        Integer value = ChangeExtension.addAndGet(gigaSpace, query, path, delta);

        Assert.assertEquals((Integer) result, value);
        ArgumentCaptor<ChangeSet> argument = ArgumentCaptor.forClass(ChangeSet.class);
        verify(gigaSpace).change(eq(query), argument.capture(), eq(ChangeModifiers.RETURN_DETAILED_RESULTS), eq(0L));
        assertIncrementOperationInChangeSet(delta, path, argument);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddAndGetNullQueryThrowException() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        int delta = 2;
        String path = "path";
        IdQuery<Object> query = null;
        ChangeExtension.addAndGet(gigaSpace, query, path, delta);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddAndGetNullPathThrowException() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        int delta = 2;
        String path = null;
        IdQuery<ChangeExtensionPojo> query = new IdQuery<ChangeExtensionPojo>(ChangeExtensionPojo.class, "id");
        ChangeExtension.addAndGet(gigaSpace, query, path, delta);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddAndGetNullDeltaThrowException() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        Integer delta = null;
        String path = "path";
        IdQuery<ChangeExtensionPojo> query = new IdQuery<ChangeExtensionPojo>(ChangeExtensionPojo.class, "id");
        ChangeExtension.addAndGet(gigaSpace, query, path, delta);
    }

    @Test
    public void testAddAndGetNoMatchingEntry() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        int delta = 2;
        String path = "path";
        IdQuery<ChangeExtensionPojo> query = new IdQuery<ChangeExtensionPojo>(ChangeExtensionPojo.class, "id");

        ChangeResult changeResult = mock(ChangeResult.class);
        when(changeResult.getNumberOfChangedEntries()).thenReturn(0);
        when(changeResult.getResults()).thenReturn(Collections.EMPTY_LIST);
        when(gigaSpace.change(any(ISpaceQuery.class), any(ChangeSet.class), any(ChangeModifiers.class), anyLong())).thenReturn(changeResult);

        Integer value = ChangeExtension.addAndGet(gigaSpace, query, path, delta);
        Assert.assertNull(value);
    }

    @Test(expected = ChangeException.class)
    public void testAddAndGetIllegalBroadcastResult() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        int delta = 2;
        String path = "path";
        IdQuery<ChangeExtensionPojo> query = new IdQuery<ChangeExtensionPojo>(ChangeExtensionPojo.class, "id");

        ChangeResult changeResult = mock(ChangeResult.class);
        when(changeResult.getNumberOfChangedEntries()).thenReturn(2);
        when(gigaSpace.change(any(ISpaceQuery.class), any(ChangeSet.class), any(ChangeModifiers.class), anyLong())).thenReturn(changeResult);

        ChangeExtension.addAndGet(gigaSpace, query, path, delta);
    }

    @Test
    public void testAddAndGetTimeout() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        int delta = 2;
        String path = "path";
        IdQuery<ChangeExtensionPojo> query = new IdQuery<ChangeExtensionPojo>(ChangeExtensionPojo.class, "id");

        ChangeResult changeResult = mock(ChangeResult.class);
        when(gigaSpace.change(any(ISpaceQuery.class), any(ChangeSet.class), any(ChangeModifiers.class), anyLong())).thenReturn(changeResult);

        ChangeExtension.addAndGet(gigaSpace, query, path, delta, 1, TimeUnit.SECONDS);
        verify(gigaSpace).change(eq(query), any(ChangeSet.class), eq(ChangeModifiers.RETURN_DETAILED_RESULTS), eq(1000L));
    }

    @Test
    public void testAddAndGetChangeModifier() {
        GigaSpace gigaSpace = mock(GigaSpace.class);
        int delta = 2;
        String path = "path";
        IdQuery<ChangeExtensionPojo> query = new IdQuery<ChangeExtensionPojo>(ChangeExtensionPojo.class, "id");

        ChangeResult changeResult = mock(ChangeResult.class);
        when(gigaSpace.change(any(ISpaceQuery.class), any(ChangeSet.class), any(ChangeModifiers.class), anyLong())).thenReturn(changeResult);

        ChangeExtension.addAndGet(gigaSpace, query, path, delta, ChangeModifiers.MEMORY_ONLY_SEARCH, 1, TimeUnit.SECONDS);
        ArgumentCaptor<ChangeModifiers> argument = ArgumentCaptor.forClass(ChangeModifiers.class);
        verify(gigaSpace).change(eq(query), any(ChangeSet.class), argument.capture(), eq(1000L));

        ChangeModifiers changeModifiers = argument.getValue();
        Assert.assertTrue((changeModifiers.getCode() & Modifiers.MEMORY_ONLY_SEARCH) != 0);
        Assert.assertTrue((changeModifiers.getCode() & Modifiers.RETURN_DETAILED_CHANGE_RESULT) != 0);
    }

    protected void assertIncrementOperationInChangeSet(int delta,
                                                       String property, ArgumentCaptor<ChangeSet> argument) {
        Collection<SpaceEntryMutator> mutators = ChangeSetInternalUtils.getMutators(argument.getValue());
        Assert.assertEquals(1, mutators.size());
        ChangeOperation changeOperation = Iterables.get(mutators, 0);
        Assert.assertTrue(IncrementOperation.represents(changeOperation));
        Assert.assertEquals(property, IncrementOperation.getPath(changeOperation));
        Assert.assertEquals(delta, IncrementOperation.getDelta(changeOperation));
    }

    protected ChangeOperationResult createIncrementChangeOperationResult(
            int delta, String property, int result) {
        ChangeOperationResult incrementChangeOperation = new IncrementSpaceEntryMutator(property, delta).getChangeOperationResult(result);
        return incrementChangeOperation;
    }
}
