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
import com.gigaspaces.internal.query.InValueIndexScanner;
import com.gigaspaces.internal.query.predicate.comparison.InSpacePredicate;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.SQLFunctions;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.BIND_PARAMETER;


/**
 * Represents an IN expression range
 *
 * @author anna
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class InRange
        extends Range {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    /**
     * @return the value
     */
    public Set getInValues() {
        final InSpacePredicate inSpacePredicate = (InSpacePredicate) getPredicate();
        return inSpacePredicate.getInValues();
    }

    public InRange() {
        super();
    }

    public InRange(String colName, Set inValues) {
        this(colName, null, inValues);
    }

    public InRange(String colName, FunctionCallDescription functionCallDescription, Set inValues) {
        super(colName, functionCallDescription, new InSpacePredicate(inValues));
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
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    public Range intersection(EqualValueRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do no have function name
            if (getInValues().contains(range.getValue())) {
                return range;
            } else {
                return EMPTY_RANGE;
            }
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // in values has function, equalRange not
            Set<Object> matchObjects = new HashSet<Object>();
            for (Object inValue : getInValues()) {
                //noinspection ConstantConditions
                if (SQLFunctions.apply(getFunction(), getFunctionCallDescription().setColumnValue(range.getValue())).equals(inValue)) {
                    matchObjects.add(inValue);
                }
            }
            return chooseRange(matchObjects);
        } else if (!hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // in values NOT have function, but equalRange has
            Set<Object> matchObjects = new HashSet<Object>();
            Object valueToSearch = range.getValue();
            for (Object inValue : this.getInValues()) {
                //noinspection ConstantConditions
                if (SQLFunctions.apply(range.getFunction(), range.getFunctionCallDescription().setColumnValue(inValue)).equals(valueToSearch)) {
                    matchObjects.add(inValue);
                }
            }
            return chooseRange(matchObjects);
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                for (Object inValue : this.getInValues()) {
                    if (inValue.equals(range.getValue())) {
                        return range;
                    }
                }
                return EMPTY_RANGE;
            } else { // both ranges has functions but not same functions
                return new CompositeRange(this, range);
            }
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    public Range intersection(NotEqualValueRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do not have function name
            Set<Object> matchObjects = new HashSet<Object>();
            for (Object inValue : this.getInValues()) {
                if (!inValue.equals(range.getValue())) {
                    matchObjects.add(inValue);
                }
            }
            return chooseRange(matchObjects);
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // in values has function, equalRange not
            return new CompositeRange(this, range);
        } else if (!hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // in values NOT have function, but equalRange has
            Set<Object> matchObjects = new HashSet<Object>();
            for (Object inValue : this.getInValues()) {
                //noinspection ConstantConditions
                if (!(SQLFunctions.apply(range.getFunction(), range.getFunctionCallDescription().setColumnValue(inValue)).equals(range.getValue()))) {
                    matchObjects.add(inValue);
                }
            }
            return chooseRange(matchObjects);
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                Set<Object> matchObjects = new HashSet<Object>();
                for (Object inValue : this.getInValues()) {
                    Object value = range.getValue();
                    if (!inValue.equals(value)) {
                        matchObjects.add(inValue);
                    }
                }
                return chooseRange(matchObjects);
            } else { // both ranges has functions but not same functions
                return new CompositeRange(this, range);
            }

        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    public Range intersection(IsNullRange range) {
        if (getInValues().contains(null))
            return this;
        else
            return Range.EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    public Range intersection(NotNullRange range) {
        if (getInValues().contains(null)) {
            Set inValues = new HashSet<Object>(getInValues());
            inValues.remove(null);
            return new InRange(getPath(), getFunctionCallDescription(), inValues);
        }
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.CompositeRange)
     */
    public Range intersection(InRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do not have function name
            Set<Object> matchObjects = new HashSet<Object>();
            Set myInValues = this.getInValues();
            Set otherInValues = range.getInValues();
            for (Object myInValue : myInValues) {
                if (otherInValues.contains(myInValue)) {
                    matchObjects.add(myInValue);
                }
            }
            return chooseRange(matchObjects);
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionOnlyOnOneSide(this, range)) {
            Set<Object> matchObjects = new HashSet<Object>();
            if (hasFunctionCallDescription(this)) {// left side has function
                for (Object rangeInValue : range.getInValues()) {
                    if (getInValues().contains(SQLFunctions.apply(getFunction(), getFunctionCallDescription().setColumnValue(rangeInValue)))) {
                        matchObjects.add(rangeInValue);
                    }
                }
                return chooseRange(matchObjects);
            } else { // right side has function
                for (Object inValue : getInValues()) {
                    FunctionCallDescription ctx = range.getFunctionCallDescription().setColumnValue(inValue);
                    SqlFunction function = range.getFunction();
                    Object o = SQLFunctions.apply(function, ctx);
                    if (range.getInValues().contains(o)) {
                        matchObjects.add(inValue);
                    }
                }
                return chooseRange(matchObjects);
            }
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            return new CompositeRange(this, range);
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    @SuppressWarnings("SpellCheckingInspection")
    public void toEntryPacket(QueryTemplatePacket e, int index) {

    }

    public Range intersection(NotRegexRange range) {
        return new CompositeRange(range, this);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
     */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        StringBuilder sqlQuerybuilder = new StringBuilder(getPath());
        sqlQuerybuilder.append(" in (");

        for (Iterator iterator = getInValues().iterator(); iterator.hasNext(); ) {
            iterator.next();
            sqlQuerybuilder.append(BIND_PARAMETER);
            if (iterator.hasNext())
                sqlQuerybuilder.append(",");

        }
        sqlQuerybuilder.append(")");
        SQLQuery query = new SQLQuery(typeDesc.getTypeName(), sqlQuerybuilder.toString());

        int index = 0;
        for (Object inValue : getInValues()) {
            query.setParameter(++index, inValue);

        }
        return query;
    }


    @Override
    public Range intersection(RegexRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public Range intersection(RelationRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        //noinspection unchecked
        return new InValueIndexScanner(getPath(), getInValues());
    }
}
