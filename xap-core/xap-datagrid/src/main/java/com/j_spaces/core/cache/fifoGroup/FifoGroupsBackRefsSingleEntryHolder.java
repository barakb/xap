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

package com.j_spaces.core.cache.fifoGroup;

import com.j_spaces.kernel.IStoredList;

@com.gigaspaces.api.InternalApi
public class FifoGroupsBackRefsSingleEntryHolder {
    private final IStoredList _singleEntry;
    private final Object _groupValue;

    public FifoGroupsBackRefsSingleEntryHolder(IStoredList singleEntry, Object groupValue) {
        _singleEntry = singleEntry;
        _groupValue = groupValue;
    }

    @Override
    public int hashCode() {
        return _singleEntry.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;
        if (!(other instanceof FifoGroupsBackRefsSingleEntryHolder))
            return false;
        FifoGroupsBackRefsSingleEntryHolder o = (FifoGroupsBackRefsSingleEntryHolder) other;
        return (_singleEntry == o._singleEntry && _groupValue.equals(o._groupValue));
    }


}
