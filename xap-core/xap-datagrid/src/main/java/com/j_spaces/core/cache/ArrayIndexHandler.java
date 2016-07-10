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

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */
/* common methods for array indexing*/

@com.gigaspaces.api.InternalApi
public class ArrayIndexHandler<K>
        extends AbstractMultiValueIndexHandler<K> {
    public ArrayIndexHandler(TypeDataIndex<K> typeDataIndex) {
        super(typeDataIndex);
    }

    protected int multiValueSize(Object mvo) {
        return Array.getLength(mvo);
    }

    protected Iterator<K> multiValueIterator(Object mvo) {
        return new ArrayIterator<K>(mvo);
    }


    private static class ArrayIterator<K>
            implements Iterator<K> {// note- we enforce calling hasNext() before next()
        private final Object _mva;
        private boolean _calledHasNext;
        private int _pos = -1;
        int _len;

        ArrayIterator(Object mva) {
            _mva = mva;
            _len = Array.getLength(_mva);
        }

        public boolean hasNext() {
            _calledHasNext = true;
            //find the next non null
            for (int i = _pos + 1; i < _len; i++) {
                if (Array.get(_mva, i) != null)
                    return true;

                _pos = i;
            }
            return false;
        }

        public K next() {
            if (!_calledHasNext)
                throw new UnsupportedOperationException();
            _calledHasNext = false;
            if (_pos < _len - 1)
                return (K) Array.get(_mva, ++_pos);

            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
