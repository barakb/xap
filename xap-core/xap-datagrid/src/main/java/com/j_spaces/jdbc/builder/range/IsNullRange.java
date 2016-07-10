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
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.QueryIndexes;
import com.gigaspaces.internal.query.predicate.comparison.NullSpacePredicate;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import static com.j_spaces.core.client.TemplateMatchCodes.IS_NULL;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.mapCodeToSign;

/**
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class IsNullRange extends Range {
    // serialVersionUID should never be changed. 
    private static final long serialVersionUID = 1L;

    public IsNullRange() {
        super();
    }

    public IsNullRange(String colName) {
        this(colName, null);
    }

    public IsNullRange(String colName, FunctionCallDescription functionCallDescription) {
        super(colName, functionCallDescription, new NullSpacePredicate());
    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    public void toEntryPacket(QueryTemplatePacket e, int index) {
        //noinspection deprecation
        e.setExtendedMatchCode(index, TemplateMatchCodes.IS_NULL);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.Range)
     */
    public Range intersection(Range range) {

        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.SegmentRange)
     */
    public Range intersection(SegmentRange range) {
        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    public Range intersection(EqualValueRange range) {
        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    public Range intersection(NotEqualValueRange range) {
        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    public Range intersection(IsNullRange range) {
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    public Range intersection(NotNullRange range) {
        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    public Range intersection(RegexRange range) {

        return EMPTY_RANGE;
    }


    public Range intersection(NotRegexRange range) {
        return this;
    }

    @Override
    public Range intersection(InRange range) {
        if (range.getInValues().contains(null))
            return range;
        else
            return Range.EMPTY_RANGE;
    }

    @Override
    public Range intersection(RelationRange range) {
        return EMPTY_RANGE;
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        return QueryIndexes.newNullValueIndexScanner(getPath());
    }


    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
     */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        return new SQLQuery(typeDesc.getTypeName(), getPath() + mapCodeToSign(IS_NULL));
    }

    @Override
    public boolean isRelevantForAllIndexValuesOptimization() {
        return true;
    }

}
