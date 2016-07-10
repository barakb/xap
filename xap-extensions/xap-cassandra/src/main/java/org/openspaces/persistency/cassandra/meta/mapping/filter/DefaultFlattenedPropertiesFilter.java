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

package org.openspaces.persistency.cassandra.meta.mapping.filter;

/**
 * A {@link FlattenedPropertiesFilter} implementation that will return <code>true</code> for all
 * fixed properties and <code>false</code> for all dynamic properties.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class DefaultFlattenedPropertiesFilter implements FlattenedPropertiesFilter {

    private static final int MAX_NESTING_LEVEL = 10;

    @Override
    public boolean shouldFlatten(PropertyContext propertyContext) {
        return !propertyContext.isDynamic() &&
                !(propertyContext.getCurrentNestingLevel() > MAX_NESTING_LEVEL);
    }

}
