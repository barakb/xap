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

package com.j_spaces.core.cache;

/**
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *
 * IOrderedIndex defines the interface for an ordered index All key objects should extend Comparable
 * values stored are Stored-lists that contains original objects with same key if hash-index is
 * already used. This is set in the Objects constructor.
 *
 * IOrderedIndex defines the interface for an ordered index All key objects should extend Comparable
 * values stored are Stored-lists that contains original objects with same key if hash-index is
 * already used. This is set in the Objects constructor.
 */

/**
 * IOrderedIndex defines the interface for an ordered index
 * All key objects should extend Comparable
 * values stored are Stored-lists that contains original objects
 *    with same key if hash-index is already used. This is set
 *    in the Objects constructor.
 */

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.kernel.IObjectInfo;

public interface IExtendedIndex<K, V>
        extends IExtendedIndexScanPositioner<K, V> {

    public IObjectInfo insertEntryIndexedField(V Entry, K fieldValue, TypeData pType, boolean alreadyCloned);

    public void removeEntryIndexedField(IEntryHolder eh,
                                        K fieldValue, V Entry, IObjectInfo oi);


}