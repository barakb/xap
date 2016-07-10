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

import java.util.AbstractList;
import java.util.List;

/**
 * An unmodifiable {@link List} holding a single element. Intended to simplify code dealing with a
 * List that in some cases may hold only one element and in others multiple elements.
 *
 * @param <E> Element to hold in list.
 * @author moran
 * @since 6.5
 */
public final class SingleElementList<E> extends AbstractList<E> {

    private final E element;

    /**
     * Constructs a {@link List} holding one and only one element.
     *
     * @param element The element the list should hold.
     */
    public SingleElementList(E element) {
        this.element = element;
    }

    @Override
    public final E get(int index) {
        if (index == 0)
            return element;
        else
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: "
                    + size());
    }

    @Override
    public final int size() {
        return 1;
    }
}
