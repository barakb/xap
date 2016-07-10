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

package com.j_spaces.core;

import com.gigaspaces.internal.server.storage.IEntryHolder;


/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

/**
 * a comparator for entries TreeMap, the objects are EntryHolders.
 */

@com.gigaspaces.api.InternalApi
public class FifoEntriesComparator implements java.util.Comparator<IEntryHolder> {
    /**
     * Compare 2 entries.
     */
    public int compare(IEntryHolder o1, IEntryHolder o2) {

        int res = compare_impl(o1, o2);
        if (res != 0)
            return res;
        if (o1 != o2 && o1.getServerTypeDesc() == o2.getServerTypeDesc() && !o1.getUID().equals(o2.getUID()))
            throw new RuntimeException("invalid fifo order 2 equal entries from same class uid1=" + o1.getUID() + " uid2=" + o2.getUID());

        return o1.getUID().compareTo(o2.getUID());
    }

    private int compare_impl(IEntryHolder o1, IEntryHolder o2) {
        if (o1.getSCN() < o2.getSCN())
            return -1;
        if (o1.getSCN() > o2.getSCN())
            return 1;
        //equal timestamp, use order
        return (o1.getOrder() - o2.getOrder());
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof FifoEntriesComparator);
    }


}