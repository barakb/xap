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

package com.gigaspaces.internal.utils.concurrent;

/**
 * @author Niv Ingberg
 * @since 9.1.2
 */
@com.gigaspaces.api.InternalApi
public class VolatileArray<T> {
    private final ElementWrapper<T>[] _array;

    public VolatileArray(int length) {
        this._array = new ElementWrapper[length];
        for (int i = 0; i < _array.length; i++)
            this._array[i] = new ElementWrapper<T>();
    }

    public int length() {
        return _array.length;
    }

    public T get(int index) {
        return _array[index].element;
    }

    public void set(int index, T value) {
        _array[index].element = value;
    }

    private static class ElementWrapper<T> {
        public volatile T element;
    }
}
