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

import com.j_spaces.kernel.list.IScanListIterator;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.5
 */
public interface IExtendedIndexScanPositioner<K, T> {
    /**
     * establish a scan according to the relation given and startPos : the start-scan object ,  null
     * means scan all values. The relation is from com.j_spaces.client.TemplateMatchCodes: LT, LE,
     * GT, GE (other codes are not relevant) endPos- key up to (or null if no limit in  index)
     * endPosInclusive : is the endPos up to (or down to) and including ? returns an
     * IOrderedIndexScan object which enables scanning the ordered index, Null if no relevant
     * elements to scan
     */
    public IScanListIterator<T> establishScan(K startPos, short relation, K endPos, boolean endPosInclusive);

    /**
     * establish a scan according to the relation given and startPos : the start-scan object ,  null
     * means scan all values. The relation is from com.j_spaces.client.TemplateMatchCodes: LT, LE,
     * GT, GE (other codes are not relevant) endPos- key up to (or null if no limit in  index)
     * endPosInclusive : is the endPos up to (or down to) and including ? ordered - according to the
     * condition. GT, GE ==> ascending, LT, LE =====> descending. returns an IOrderedIndexScan
     * object which enables scanning the ordered index, Null if no relevant elements to scan
     */
    public IScanListIterator<T> establishScan(K startPos, short relation, K endPos, boolean endPosInclusive, boolean ordered);
}
