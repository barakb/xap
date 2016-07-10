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

import com.gigaspaces.internal.server.space.MatchTarget;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;

/**
 * Iterator for templates locked under Xtn.
 */
@com.gigaspaces.api.InternalApi
public class UnderXtnTemplatesIter extends SAIterBase
        implements ISAdapterIterator<ITemplateHolder> {
    final private IStoredList<TemplateCacheInfo> _templates;
    private IStoredListIterator<TemplateCacheInfo> _pos;  // position in SL
    private TemplateCacheInfo _currentTemplate;

    public UnderXtnTemplatesIter(Context context, XtnEntry xtnEntry, MatchTarget matchTarget, CacheManager cacheManager)
            throws SAException, NullIteratorException {
        super(context, cacheManager);

        // init. m_Templates SVector
        XtnData pXtn = xtnEntry.getXtnData();
        if (pXtn == null) {
            close();
            throw new NullIteratorException();
        }
        _templates = matchTarget == MatchTarget.NOTIFY ? pXtn.getNTemplates() : pXtn.getRTTemplates();
        if (_templates != null)
            _pos = _templates.establishListScan(false);
    }

    public ITemplateHolder next()
            throws SAException {
        checkIfNext();

        return _currentTemplate == null ? null : _currentTemplate.m_TemplateHolder;
    }

    /**
     * checks if there is a valid next element and sets the m_Pos and m_CurrentTemplate fields
     * accordingly.
     */
    private void checkIfNext() {
        for (; _pos != null; _pos = _templates.next(_pos)) {
            TemplateCacheInfo pTemplate = _pos.getSubject();
            if (pTemplate == null)
                continue;
            if (pTemplate.m_TemplateHolder.isDeleted())
                continue;

            _currentTemplate = pTemplate;
            _pos = _templates.next(_pos);
            return;
        }

        _currentTemplate = null;
    }

    /**
     * overrides com.j_spaces.core.cache.SAIterBase.close()
     */
    @Override
    public void close() throws SAException {
        if (_templates != null)
            _templates.freeSLHolder(_pos);

        super.close();
    }
}