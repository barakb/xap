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

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */
/* common methods for collection indexing*/

@com.gigaspaces.api.InternalApi
public class CollectionIndexHandler<K>
        extends AbstractMultiValueIndexHandler<K> {
    public CollectionIndexHandler(TypeDataIndex<K> typeDataIndex) {
        super(typeDataIndex);
    }

    protected int multiValueSize(Object mvo) {
        return ((Collection) mvo).size();
    }

    protected Iterator<K> multiValueIterator(Object mvo) {
        return ((Collection<K>) mvo).iterator();
    }

}
