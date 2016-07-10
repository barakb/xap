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

import java.util.Collection;
import java.util.Iterator;

@com.gigaspaces.api.InternalApi
public class GeneralMultiValueIndexHandler<K>
        extends AbstractMultiValueIndexHandler<K> {
    private final ArrayIndexHandler<K> _arrayHandler;

    public GeneralMultiValueIndexHandler(TypeDataIndex<K> typeDataIndex) {
        super(typeDataIndex);
        _arrayHandler = new ArrayIndexHandler<K>(typeDataIndex);
    }

    protected int multiValueSize(Object mvo) {
        return !(mvo instanceof Collection) ? _arrayHandler.multiValueSize(mvo) : ((Collection) mvo).size();
    }

    protected Iterator<K> multiValueIterator(Object mvo) {
        return !(mvo instanceof Collection) ? _arrayHandler.multiValueIterator(mvo) : ((Collection<K>) mvo).iterator();
    }

}
