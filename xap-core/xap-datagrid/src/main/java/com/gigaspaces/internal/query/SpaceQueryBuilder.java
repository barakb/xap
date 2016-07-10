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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.query.predicate.ISpacePredicate;
import com.gigaspaces.internal.query.predicate.composite.AllSpacePredicate;
import com.gigaspaces.query.ISpaceQuery;

/**
 * Provides methods to create various space queries.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class SpaceQueryBuilder {
    /**
     * Creates a space query using the specified class and predicate.
     *
     * @param entryClass Class to query.
     * @param predicate  Predicate for matching entries.
     * @return the created space query
     */
    public static ISpaceQuery create(Class<?> entryClass, ISpacePredicate predicate) {
        return new CustomSpaceQuery(entryClass, predicate);
    }

    /**
     * Creates a space query using the specified class and predicates. The predicates are
     * concatenated in an All predicate.
     *
     * @param entryClass Class to query.
     * @param predicates Predicates to match.
     * @return the created space query
     */
    public static ISpaceQuery create(Class<?> entryClass, ISpacePredicate... predicates) {
        return new CustomSpaceQuery(entryClass, new AllSpacePredicate(predicates));
    }
}
