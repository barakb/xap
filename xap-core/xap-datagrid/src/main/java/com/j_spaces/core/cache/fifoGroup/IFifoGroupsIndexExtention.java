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

package com.j_spaces.core.cache.fifoGroup;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;

import java.util.ArrayList;

public interface IFifoGroupsIndexExtention<K> {
    void insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType);

    void insertEntryIndexedField(IEntryCacheInfo pEntry, K fieldValue, TypeData pType, ArrayList<IObjectInfo<IEntryCacheInfo>> insertBackRefs);

    void addToValuesList(K groupValue, IStoredList list);

    void removeFromValuesList(K groupValue, IStoredList list);

    int removeEntryIndexedField(IEntryHolder eh, ArrayList<IObjectInfo<IEntryCacheInfo>> deletedBackRefs,
                                K fieldValue, int refpos, boolean removeIndexedValue, IEntryCacheInfo pEntry);

    IStoredList getFifoGroupLists();

    IStoredList getFifoGroupLists(Object otherIndexValue);

    int getNumGroups();
}
