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

package com.j_spaces.kernel;

/**
 * Safe use "array", to use when {@link ArrayIndexOutOfBoundsException} might accord. Notice: This
 * implementation is not Thread safe.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.1
 **/
@com.gigaspaces.api.InternalApi
public class SafeArray<T> implements ISafeArray<T> {
    private T[] _elements;

    /**
     * Constructs an empty safe array with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list.
     * @throws IllegalArgumentException if the specified initial capacity is negative
     **/
    public SafeArray(int initialCapacity) {
        _elements = (T[]) new Object[initialCapacity];
    }

    public SafeArray(SafeArray<T> copy) {
        T[] localElements = copy._elements;
        _elements = (T[]) new Object[localElements.length];
        System.arraycopy(localElements, 0, _elements, 0, localElements.length);
    }

    /**
     * Constructs an empty safe array with an initial capacity of ten.
     **/
    public SafeArray() {
        this(10);
    }


    /*
   * @see com.j_spaces.kernel.ISafeArray#get(int)
   */
    public T get(int index) {
        if (_elements.length <= index)
            return null;

        return _elements[index];
    }


    /*
   * @see com.j_spaces.kernel.ISafeArray#add(int, T)
   */
    public void add(int index, T element) {
        if (_elements.length <= index) // need to increase the array
        {
            int newSize = (index * 3) / 2 + 1;
            if (newSize < 0) // overflow
            {
                if (index < Integer.MAX_VALUE) // increase as much as possible
                    newSize = Integer.MAX_VALUE;
                else
                    throw new ArrayIndexOutOfBoundsException("Can't add element index is legal: " + index);

            }
            T[] newElements = (T[]) new Object[newSize];
            System.arraycopy(_elements, 0, newElements, 0, _elements.length);
            _elements = newElements;
        }
        _elements[index] = element;
    }

    public void clear() {
        _elements = (T[]) new Object[10];
    }
}
