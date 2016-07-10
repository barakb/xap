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
import com.gigaspaces.internal.query.NotRegexIndexScanner;
import com.gigaspaces.internal.query.predicate.comparison.NotRegexSpacePredicate;
import com.j_spaces.core.client.SQLQuery;

import static com.j_spaces.core.client.TemplateMatchCodes.REGEX;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.BIND_PARAMETER;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.convertToSQLFormat;

/**
 * Represents a negation of a regular expression range
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class NotRegexRange extends SingleValueRange {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    public NotRegexRange() {
        super();
    }

    public NotRegexRange(String colName, String value) {
        this(colName, null, value);
    }

    public NotRegexRange(String colName, FunctionCallDescription functionCallDescription, String value) {
        super(colName, functionCallDescription, value, new NotRegexSpacePredicate(value));
    }

    /*
     * (non-Javadoc)
	 * 
	 * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.Range)
	 */
    public Range intersection(Range range) {
        return range.intersection(this);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.SegmentRange)
     */
    public Range intersection(SegmentRange range) {
        return new CompositeRange(range, this);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    public Range intersection(EqualValueRange range) {
        return new CompositeRange(range, this);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    public Range intersection(NotEqualValueRange range) {
        return new CompositeRange(this, range);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    public Range intersection(IsNullRange range) {
        return EMPTY_RANGE;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    public Range intersection(NotNullRange range) {
        return this;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.CompositeRange)
     */
    public Range intersection(NotRegexRange range) {
        if (range.getValue().equals(getValue()))
            return this;
        return new CompositeRange(this, range);
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    public Range intersection(RegexRange range) {
        if (range.getValue().equals(getValue()))
            return EMPTY_RANGE;

        return new CompositeRange(range, this);
    }

    @Override
    public Range intersection(InRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public Range intersection(RelationRange range) {
        return new CompositeRange(range, this);
    }

    /* (non-Javadoc)
         * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
         */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        return new SQLQuery(typeDesc.getTypeName(), getPath() + " not like " + BIND_PARAMETER, convertToSQLFormat(getValue(), REGEX));
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        return new NotRegexIndexScanner(getPath(), (String) getValue());
    }

    @Override
    public boolean isRelevantForAllIndexValuesOptimization() {
        return false;
    }
}
