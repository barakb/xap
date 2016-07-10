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

package com.j_spaces.jdbc.builder.range;

import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;


/**
 * Collection contains composite range.
 *
 * @author idan
 * @since 8.0.1
 */
@com.gigaspaces.api.InternalApi
public class ContainsCompositeRange extends CompositeRange {
    private static final long serialVersionUID = 2495470012301024060L;

    public ContainsCompositeRange() {
        super();
    }

    public ContainsCompositeRange(ContainsValueRange containsValueRange, Range range) {
        super(containsValueRange, range);
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        return getPredicate().execute(entry);
    }

}
