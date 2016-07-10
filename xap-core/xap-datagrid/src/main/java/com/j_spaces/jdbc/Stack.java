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

package com.j_spaces.jdbc;

import java.util.LinkedList;


/**
 * Simple implementation of Stack data structure.
 *
 * The Stack is not thread safe and intended for single thread usage
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class Stack<E> extends LinkedList<E> {
    private static final long serialVersionUID = -9032364299490917451L;

    /**
     * Creates an empty Stack.
     */
    public Stack() {
    }

    public void push(E item) {
        addFirst(item);
    }

    public E pop() {
        return removeFirst();
    }
}
