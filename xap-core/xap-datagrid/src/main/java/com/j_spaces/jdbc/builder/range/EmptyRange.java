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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.SQLQuery;

/**
 * Represents an empty range - range that always translates to an empty query
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class EmptyRange extends Range {
    // serialVersionUID should never be changed. 
    private static final long serialVersionUID = 1L;

    public EmptyRange() {
        super();
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange#intersection(com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange)
     */
    public Range intersection(Range range) {
        return this;
    }

    @Override
    public Range intersection(CompositeRange range) {
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.SegmentRange)
     */
    @Override
    public Range intersection(SegmentRange range) {
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    @Override
    public Range intersection(EqualValueRange range) {

        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    @Override
    public Range intersection(NotEqualValueRange range) {

        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    @Override
    public Range intersection(IsNullRange range) {

        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    @Override
    public Range intersection(NotNullRange range) {

        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    @Override
    public Range intersection(RegexRange range) {

        return this;
    }

    @Override
    public Range intersection(NotRegexRange range) {
        return this;
    }

    @Override
    public Range intersection(InRange range) {
        return this;
    }

    @Override
    public Range intersection(RelationRange range) {
        return this;
    }

    /* (non-Javadoc)
         * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#toSQLQuery(com.gigaspaces.internal.metadata.ITypeDesc)
         */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        throw new UnsupportedOperationException("Empty range can't be converted to SQLQuery");
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        // shouldn't get here, just a precaution
        return false;
    }

    @Override
    public boolean isEmptyRange() {
        return true;
    }


}
