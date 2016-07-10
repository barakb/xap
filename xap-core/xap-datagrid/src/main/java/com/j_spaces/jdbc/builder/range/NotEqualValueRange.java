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
import com.gigaspaces.internal.query.predicate.comparison.NotEqualsSpacePredicate;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.BIND_PARAMETER;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.mapCodeToSign;

@com.gigaspaces.api.InternalApi
public class NotEqualValueRange extends SingleValueRange {
    // serialVersionUID should never be changed. 
    private static final long serialVersionUID = 1L;

    public NotEqualValueRange() {
        super();
    }

    public NotEqualValueRange(String colName, FunctionCallDescription functionCallDescription, Object value) {
        super(colName, functionCallDescription, value, new NotEqualsSpacePredicate(value));
    }

    public NotEqualValueRange(String colName, Object value) {
        this(colName, null, value);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    public void toEntryPacket(QueryTemplatePacket e, int index) {
        e.setFieldValue(index, getValue());
        //noinspection deprecation
        e.setExtendedMatchCode(index, TemplateMatchCodes.NE);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange#intersection(com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange)
     */
    public Range intersection(Range range) {
        return range.intersection(this);
    }

    public Range intersection(NotEqualValueRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do not have function name
            return checkValueEqauls(range);
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionOnlyOnOneSide(this, range)) {
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                return checkValueEqauls(range);
            } else { // both ranges has functions but different functions
                return new CompositeRange(this, range);
            }
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    private Range checkValueEqauls(NotEqualValueRange range) {
        if (this.getValue().equals(range.getValue())) {
            return this;
        } else {
            return new CompositeRange(this, range);
        }
    }

    public Range intersection(SegmentRange range) {
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    public Range intersection(EqualValueRange range) {
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    public Range intersection(IsNullRange range) {
        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    public Range intersection(NotNullRange range) {

        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    public Range intersection(RegexRange range) {
        return new CompositeRange(this, range);
    }

    public Range intersection(NotRegexRange range) {
        return new CompositeRange(this, range);
    }

    @Override
    public Range intersection(InRange range) {
        return range.intersection(this);
    }

    @Override
    public Range intersection(RelationRange range) {
        return new CompositeRange(this, range);
    }

    /* (non-Javadoc)
         * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
         */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        //noinspection deprecation
        return new SQLQuery(typeDesc.getTypeName(), getPath() + mapCodeToSign(TemplateMatchCodes.NE) + BIND_PARAMETER, getValue());
    }

    @Override
    public boolean isRelevantForAllIndexValuesOptimization() {
        return false;
    }

}