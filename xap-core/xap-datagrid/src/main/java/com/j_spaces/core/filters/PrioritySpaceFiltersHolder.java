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

package com.j_spaces.core.filters;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class PrioritySpaceFiltersHolder {
    public final boolean hasFilterRequiresFullSpaceFilterEntry;
    public final FilterHolder[][] prioritizedFilterHolders;
    public final boolean isSingleFilterHolder;
    public final FilterHolder singleFilterHolder;

    public PrioritySpaceFiltersHolder(FilterHolder[][] prioritizedFilterHolders) {
        this.prioritizedFilterHolders = prioritizedFilterHolders;

        boolean tempHasFilterRequiresFullSpaceFilterEntry = false;
        FilterHolder tempSingleFilterHolder = null;
        boolean tempIsSingleFilterHolder = true;

        for (FilterHolder[] filterHolders : prioritizedFilterHolders) {
            if (filterHolders == null)
                continue;

            for (FilterHolder filterHolder : filterHolders) {
                //Check whether we already encountered a filter before, if so this is not a single filter holder case
                if (tempIsSingleFilterHolder && tempSingleFilterHolder != null) {
                    tempIsSingleFilterHolder = false;
                    tempSingleFilterHolder = null;
                } else {
                    tempSingleFilterHolder = filterHolder;
                }

                tempHasFilterRequiresFullSpaceFilterEntry |= filterHolder.isPassFilterEntry();
            }
        }

        this.hasFilterRequiresFullSpaceFilterEntry = tempHasFilterRequiresFullSpaceFilterEntry;
        this.singleFilterHolder = tempSingleFilterHolder;
        this.isSingleFilterHolder = tempIsSingleFilterHolder;
    }
}
