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

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;

/**
 * Iterator for entries locked under a Xtn.
 */
@com.gigaspaces.api.InternalApi
public class UnderXtnEntriesIter
        extends SAIterBase
        implements ISAdapterIterator {
    //if true PEntry is used returned and not entryHolder
    final private boolean _returnPEntry;
    final private IStoredList<IEntryCacheInfo> _entries;
    private IStoredListIterator<IEntryCacheInfo> _pos;  // position in SL
    private IEntryCacheInfo _currentEntry;

    /**
     *
     * @param context
     * @param xtnEntry
     * @param selectType
     * @param cacheManager
     * @param returnPEntry
     * @throws SAException
     * @throws NullIteratorException
     */
    public UnderXtnEntriesIter(Context context, XtnEntry xtnEntry,
                               int selectType, CacheManager cacheManager, boolean returnPEntry)
            throws SAException, NullIteratorException {
        super(context, cacheManager);

        // init. m_Entries SVector
        XtnData pXtn = xtnEntry.getXtnData();
        if (pXtn == null) {
            close();
            throw new NullIteratorException();
        }
        _returnPEntry = returnPEntry;
        _entries = pXtn.getUnderXtnEntries(selectType);
        if (_entries != null)
            _pos = _entries.establishListScan(false);
    }

    public Object next()
            throws SAException {
        checkIfNext();

        if (_returnPEntry)
            return _currentEntry;

        return _currentEntry == null ? null : _currentEntry.getEntryHolder(_cacheManager);
    }

    /**
     * checks if there is a valid next element and sets the m_Pos and m_CurrentEntry fields
     * accordingly.
     */
    private void checkIfNext() {
        for (; _pos != null; _pos = _entries.next(_pos)) {
            IEntryCacheInfo pEntry = _pos.getSubject();
            if (pEntry == null)
                continue;

            _currentEntry = pEntry;
            _pos = _entries.next(_pos);
            return;
        }

        _currentEntry = null;
    }

    /**
     * overrides com.j_spaces.core.cache.SAIterBase.close()
     */
    @Override
    public void close() throws SAException {
        if (_entries != null)
            _entries.freeSLHolder(_pos);

        super.close();
    }
}