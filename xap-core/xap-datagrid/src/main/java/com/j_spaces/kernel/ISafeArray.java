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
 * @author eitany
 * @since 7.0
 */
public interface ISafeArray<T> {

    /**
     * Returns the element at the specified position in this list.
     *
     * @param index index of element to return.
     * @return the element at the specified position in this list.
     **/
    T get(int index);

    /**
     * Inserts the specified element at the specified position in this list.
     *
     * @param index   index at which the specified element is to be inserted.
     * @param element element to be inserted.
     **/
    void add(int index, T element);

    /**
     * Clears the array
     */
    void clear();
}