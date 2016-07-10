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
import com.gigaspaces.internal.query.predicate.comparison.NotNullSpacePredicate;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import static com.j_spaces.core.client.TemplateMatchCodes.NOT_NULL;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.mapCodeToSign;

/**
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class NotNullRange extends Range {
    // serialVersionUID should never be changed. 
    private static final long serialVersionUID = 1L;

    public NotNullRange() {
        super();
    }

    public NotNullRange(String colName) {
        this(colName, null);
    }

    public NotNullRange(String colName, FunctionCallDescription functionCallDescription) {
        super(colName, functionCallDescription, new NotNullSpacePredicate());
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    public void toEntryPacket(QueryTemplatePacket e, int index) {
        //noinspection deprecation
        e.setExtendedMatchCode(index, TemplateMatchCodes.NOT_NULL);
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
        return range;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    public Range intersection(EqualValueRange range) {
        return range;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    public Range intersection(NotEqualValueRange range) {
        return range;
    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    public Range intersection(NotNullRange range) {
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    public Range intersection(IsNullRange range) {

        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    public Range intersection(RegexRange range) {

        return range;
    }


    public Range intersection(NotRegexRange range) {
        return range;
    }

    @Override
    public Range intersection(InRange range) {
        return range.intersection(this);
    }

    @Override
    public Range intersection(RelationRange range) {
        return range;
    }

    /* (non-Javadoc)
         * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
         */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        return new SQLQuery(typeDesc.getTypeName(), getPath() + mapCodeToSign(NOT_NULL));
    }
}
