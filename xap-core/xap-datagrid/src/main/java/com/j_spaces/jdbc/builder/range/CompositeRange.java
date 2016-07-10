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
import com.gigaspaces.internal.query.predicate.composite.AllSpacePredicate;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

/**
 * CompositeRange indicates a result of a complex intersection that can't be translated to a single
 * range. In this case  the first range is executed as a query and the remaining ranges used to
 * filter the irrelevant results.
 *
 * This is not very efficient but used only in very specific case:
 *
 * x > 3 and x < 6 and x <> 4
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class CompositeRange extends Range {
    // serialVersionUID should never be changed. 
    private static final long serialVersionUID = 1L;

    // list of ranges that will be used to filter the results
    private LinkedList<Range> _ranges = new LinkedList<Range>();

    public CompositeRange() {
        super();
    }

    public CompositeRange(Range r1, Range r2) {
        super(r1.getPath(), null, new AllSpacePredicate());

        add(r1);
        add(r2);
    }

    public CompositeRange(String path) {
        super(path, null, new AllSpacePredicate());
    }

    /**
     * Add a range to the composite ranges
     */
    public CompositeRange add(Range range) {
        _ranges.add(range);
        ((AllSpacePredicate) this.getPredicate()).addOperand(range.getPredicate());
        return this;

    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.Range)
     */
    @Override
    public Range intersection(Range range) {
        CompositeRange newRange = new CompositeRange(getPath());
        for (Range r : _ranges)
            newRange.add(r);
        newRange.add(range);
        return newRange;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.SegmentRange)
     */
    @Override
    public Range intersection(SegmentRange range) {
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    @Override
    public Range intersection(EqualValueRange range) {
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    @Override
    public Range intersection(NotEqualValueRange range) {
        return range.intersection(this);
    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    @Override
    public Range intersection(NotNullRange range) {
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    @Override
    public Range intersection(IsNullRange range) {
        return range.intersection(this);
    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
	 */
    @Override
    public void toEntryPacket(QueryTemplatePacket e, int index) {

        // Use the first range as the query range - the others will be filtered later
        _ranges.getFirst().toEntryPacket(e, index);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    @Override
    public Range intersection(RegexRange range) {
        return range.intersection(this);
    }

    @Override
    public Range intersection(RelationRange range) {
        return add(range);
    }

    @Override
    public Range intersection(NotRegexRange range) {
        return range.intersection(this);
    }

    @Override
    public Range intersection(InRange range) {
        return range.intersection(this);
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public boolean isRelevantForAllIndexValuesOptimization() {
        boolean res = true;
        if (_ranges != null)
            for (Range r : _ranges)
                res &= r.isRelevantForAllIndexValuesOptimization();

        return res;
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        //noinspection unchecked
        _ranges = (LinkedList<Range>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        out.writeObject(_ranges);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
     */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {

        List preparedValues = new LinkedList();
        StringBuilder b = new StringBuilder();
        for (Range query : _ranges) {
            SQLQuery sqlQuery = query.toSQLQuery(typeDesc);

            if (b.length() > 0)
                b.append(DefaultSQLQueryBuilder.AND);
            b.append(sqlQuery.getQuery());

            if (sqlQuery.getParameters() == null)
                continue;

            //noinspection ManualArrayToCollectionCopy
            for (int i = 0; i < sqlQuery.getParameters().length; i++) {
                //noinspection unchecked
                preparedValues.add(sqlQuery.getParameters()[i]);
            }
        }


        return new SQLQuery(typeDesc.getTypeName(), b.toString(), preparedValues.toArray());
    }

    public LinkedList<Range> get_ranges() {
        return _ranges;
    }
}
