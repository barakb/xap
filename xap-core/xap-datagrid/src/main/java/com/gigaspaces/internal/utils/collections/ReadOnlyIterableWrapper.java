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

import java.util.Iterator;

@com.gigaspaces.api.InternalApi
public class ReadOnlyIterableWrapper<T>
        implements Iterable<T> {

    private final Iterable<T> _iterable;

    public ReadOnlyIterableWrapper(
            Iterable<T> iterable) {
        _iterable = iterable;
    }

    public Iterator<T> iterator() {
        return new ReadOnlyIteratorWrapper(_iterable.iterator());
    }


    public class ReadOnlyIteratorWrapper
            implements Iterator<T> {

        private final Iterator<T> _iterator;

        public ReadOnlyIteratorWrapper(Iterator<T> iterator) {
            _iterator = iterator;
        }

        public boolean hasNext() {
            return _iterator.hasNext();
        }

        public T next() {
            return _iterator.next();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
