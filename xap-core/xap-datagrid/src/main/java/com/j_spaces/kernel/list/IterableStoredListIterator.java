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

package com.j_spaces.kernel.list;

import com.j_spaces.kernel.AbstractStoredList;
import com.j_spaces.kernel.StoredListIterator;

import java.util.Iterator;

/**
 * Iterator over an {@link AbstractStoredList}.
 *
 * @author anna
 * @version 1.0
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class IterableStoredListIterator<T>
        implements Iterator<T> {
    private final StoredListIterator _listIterator;
    private final AbstractStoredList<T> _entries;
    private T _res;

    public IterableStoredListIterator(AbstractStoredList<T> entries,
                                      boolean random_scan) {
        _entries = entries;
        _listIterator = entries.establishListScan(random_scan);
    }

    public boolean hasNext() {
        if (_listIterator == null)
            return false;
        _res = nextValue();
        return _res != null;
    }

    public T next() {
        T res = _res;
        _res = null;
        return res;
    }

    private T nextValue() {
        T res = null;
        while (true) {
            boolean hasNext = _entries.nextPos(_listIterator);
            if (hasNext) {
                res = (T) _listIterator.getSubject();
                if (res != null)
                    return res;
            }
            return null;

        }

    }

    public void remove() {
        throw new RuntimeException(" not supported");
    }

}
