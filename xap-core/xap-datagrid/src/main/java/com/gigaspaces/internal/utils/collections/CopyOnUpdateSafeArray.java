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

package com.gigaspaces.internal.utils.collections;

import com.j_spaces.kernel.ISafeArray;
import com.j_spaces.kernel.SafeArray;

/**
 * Copy on update implementation of {@link ISafeArray}
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class CopyOnUpdateSafeArray<T> implements ISafeArray<T> {
    volatile private SafeArray<T> _safeArray;

    public CopyOnUpdateSafeArray() {
        this(10);
    }

    public CopyOnUpdateSafeArray(int initialCapacity) {
        _safeArray = new SafeArray<T>(initialCapacity);
    }

    public synchronized void add(int index, T element) {
        SafeArray<T> copySafeArray = new SafeArray<T>(_safeArray);
        copySafeArray.add(index, element);

        _safeArray = copySafeArray;
    }

    public T get(int index) {
        return _safeArray.get(index);
    }

    public synchronized void clear() {
        _safeArray = new SafeArray<T>(10);
    }

}
