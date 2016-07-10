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

package com.gigaspaces.internal.query;

import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Scans only the entries in the defined range via compound index
 *
 * @author Yechiel
 * @since 9.5
 */

@com.gigaspaces.api.InternalApi
public class RangeCompoundIndexScanner extends AbstractQueryIndex {

    private static final long serialVersionUID = 1L;

    private Comparable<?>[] _min;

    private boolean _includeMin;
    private boolean _includeMinFirstSegment;
    private Comparable<?>[] _max;
    private boolean _includeMax;
    private boolean _includeMaxFirstSegment;

    private transient short _minMatchCode;
    private transient CompoundConvertedObjectWrapper _convertedMinWrapper;
    private transient CompoundConvertedObjectWrapper _convertedMaxWrapper;

    public RangeCompoundIndexScanner() {
        super();
    }

    public RangeCompoundIndexScanner(String indexName, Comparable<?>[] min,
                                     boolean includeMin, boolean includeMinFirstSegment, Comparable<?>[] max, boolean includeMax, boolean includeMaxFirstSegment) {
        super(indexName);
        _min = min;
        _includeMin = includeMin;
        _max = max;
        _includeMax = includeMax;

        _minMatchCode = initialize();
    }

    private short initialize() {
        return _includeMin ? TemplateMatchCodes.GE : TemplateMatchCodes.GT;
    }


    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        if (_convertedMinWrapper == null)
            _convertedMinWrapper = CompoundConvertedObjectWrapper.createForExtendedMatch(_min, index, true /*isMin*/, _includeMin, _includeMinFirstSegment);
        if (_convertedMinWrapper == null)
            return null;

        if (_convertedMaxWrapper == null)
            _convertedMaxWrapper = CompoundConvertedObjectWrapper.createForExtendedMatch(_max, index, false /*isMin*/, _includeMax, _includeMaxFirstSegment);
        if (_convertedMaxWrapper == null)
            return null;

        return !fifoGroupsScan ? index.getExtendedIndexForScanning().establishScan(
                _convertedMinWrapper.getValue(), _minMatchCode, _convertedMaxWrapper.getValue(), _includeMax)
                : index.getExtendedFifoGroupsIndexForScanning().establishScan(
                _convertedMinWrapper.getValue(), _minMatchCode, _convertedMaxWrapper.getValue(), _includeMax);

    }

    @Override
    protected boolean hasIndexValue() {
        return _min != null || _max != null;
    }

    public Object getIndexValue() {
        return _min;
    }

    public boolean requiresOrderedIndex() {
        return true;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        _min = (Comparable<?>[]) in.readObject();
        _includeMin = in.readBoolean();
        _includeMinFirstSegment = in.readBoolean();
        _max = (Comparable<?>[]) in.readObject();
        _includeMax = in.readBoolean();
        _includeMaxFirstSegment = in.readBoolean();

        _minMatchCode = initialize();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(_min);
        out.writeBoolean(_includeMin);
        out.writeBoolean(_includeMinFirstSegment);
        out.writeObject(_max);
        out.writeBoolean(_includeMax);
        out.writeBoolean(_includeMaxFirstSegment);
    }

    public boolean supportsTemplateIndex() {
        return false;
    }


    public static RangeCompoundIndexScanner build(String name, List<Range> possibleSegments, boolean isFirstSegmentIndexed) {
        Comparable[] min = null;
        Comparable[] max = null;
        boolean includeMin = false;
        boolean includeMax = false;
        boolean includeMinFirstSegment = false;
        boolean includeMaxFirstSegment = false;

        for (int i = 0; i < possibleSegments.size(); i++) {
            if (!possibleSegments.get(i).isSegmentRange())
                return null;

            SegmentRange r = (SegmentRange) possibleSegments.get(i);
            if (r.getMin() != null) {
                if (min == null) {
                    if (i > 0)
                        return null;
                    min = new Comparable[possibleSegments.size()];
                }
                if (i == 0) {
                    if (!r.isIncludeMin() && isFirstSegmentIndexed)
                        return null;    //not much use of compound, single index can be used
                    includeMinFirstSegment = r.isIncludeMin();
                }
                min[i] = r.getMin();
                if (r.isIncludeMin())
                    includeMin = true;
            }
            if (r.getMax() != null) {
                if (max == null) {
                    if (i > 0)
                        return null;
                    max = new Comparable[possibleSegments.size()];
                }
                if (i == 0) {
                    if (!r.isIncludeMax() && isFirstSegmentIndexed)
                        return null;    //not much use of compound, single index can be used
                    includeMaxFirstSegment = r.isIncludeMax();
                }
                max[i] = r.getMax();
                if (r.isIncludeMax())
                    includeMax = true;
            }
        }
        if (min == null && max == null)
            return null;

        return new RangeCompoundIndexScanner(name, min, includeMin, includeMinFirstSegment, max, includeMax, includeMaxFirstSegment);
    }


}
