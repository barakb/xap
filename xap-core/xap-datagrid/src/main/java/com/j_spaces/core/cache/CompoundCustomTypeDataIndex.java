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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.fifoGroup.FifoGroupsCompoundIndexExtention;
import com.j_spaces.kernel.IObjectInfo;

import java.security.InvalidParameterException;

/**
 * Custom index for a multi-segments index compoused of other (not compound) indexes
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */

@com.gigaspaces.api.InternalApi
public class CompoundCustomTypeDataIndex<K>
        extends CustomTypeDataIndex<K> {
    private final TypeDataIndex[] _segmentsFromIndexes; //if segments where derived from indexes

    private final CompoundIndexSegmentTypeData[] _segments;
    //the max position of the (root) properties of all the segments. used in templates matching
    private final int _maxFixedPropertiesSegmentsPos;

    private final boolean _considerValueClone;
    private boolean _valueTypeFinallySet;


    public static ICompoundIndexValueHolder.LowEdge _lowEdge = new ICompoundIndexValueHolder.LowEdge();
    public static ICompoundIndexValueHolder.HighEdge _highEdge = new ICompoundIndexValueHolder.HighEdge();


    public CompoundCustomTypeDataIndex(CacheManager cacheManager, ISpaceIndex index, CompoundIndexSegmentTypeData[] segments, int indexCreationNumber, int indexPosition, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType) {
        super(cacheManager, index, indexPosition /*pos*/, indexCreationNumber, fifoGroupsIndexType);

        if (segments.length == 1)
            throw new InvalidParameterException();

        _segments = segments;

        boolean considerClone = false;
        for (CompoundIndexSegmentTypeData segment : segments) {
            if (segment.isConsiderValueClone()) {
                considerClone = true;
                break;
            }
        }
        _considerValueClone = considerClone;
        TypeDataIndex[] segmentsFromIndexes = new TypeDataIndex[_segments.length];
        for (CompoundIndexSegmentTypeData segment : segments) {
            if (segment.getOriginatingIndex() != null) {
                segmentsFromIndexes[segment.getSegmentPoition() - 1] = segment.getOriginatingIndex();
            } else {
                segmentsFromIndexes = null;
                break;
            }
        }
        _segmentsFromIndexes = segmentsFromIndexes;

        int maxFixedPropertiesSegmentsPos = -1;
        for (CompoundIndexSegmentTypeData segment : segments) {
            if (segment.getFixedPropertyPos() == -1) {
                maxFixedPropertiesSegmentsPos = -1;
                break;
            }
            if (segment.getFixedPropertyPos() > maxFixedPropertiesSegmentsPos)
                maxFixedPropertiesSegmentsPos = segment.getFixedPropertyPos();
        }
        _valueTypeFinallySet = _segmentsFromIndexes != null;
        _maxFixedPropertiesSegmentsPos = maxFixedPropertiesSegmentsPos;
        if (fifoGroupsIndexType == ISpaceIndex.FifoGroupsIndexTypes.COMPOUND)
            _fifoGroupsIndexExtention = new FifoGroupsCompoundIndexExtention<K>(cacheManager, this);


    }


    @Override
    public boolean isCompound() {
        return true;
    }

    @Override
    public int getMaxFixedPropertiesSegmentPos() {
        return _maxFixedPropertiesSegmentsPos;
    }

    private int getNumSegments() {
        return _segments.length;
    }


    @Override
    public TypeDataIndex[] getSegmentsOriginatingIndexes() {
        return _segmentsFromIndexes;
    }

    @Override
    public Object getIndexValue(ServerEntry entry) {
        return getIndexDefinition().getIndexValue(entry);
    }


    @Override
    public Object getIndexValueForTemplate(ServerEntry entry) {
        return getIndexValue(entry);
    }

    @Override
//TBD should remove this method    
    public Object getCompoundIndexValueForTemplate(ServerEntry entry) {
        return getIndexValue(entry);
    }


    @Override
    protected void insertBasicIndexTemplate(TemplateCacheInfo pTemplate, boolean isNullIndex) {
    }


    @Override
    protected int removeBasicIndexTemplate(TemplateCacheInfo pTemplate,
                                           IObjectInfo<TemplateCacheInfo> oi, int refpos) {
        return refpos;
    }

    @Override
    protected boolean isConsiderValueClone() {
        return _considerValueClone;
    }

    @Override
    public CompoundIndexSegmentTypeData[] getCompoundIndexSegments() {
        return _segments;
    }

    @Override
    K cloneIndexValue(K fieldValue, IEntryHolder entryHolder) {
        if (!isConsiderValueClone() || fieldValue == null)
            return fieldValue;

        //we clone only the needed segments
        ICompoundIndexValueHolder vh = (ICompoundIndexValueHolder) fieldValue;
        for (int i = 0; i < vh.getNumSegments(); i++) {
            Object val = _segments[i].cloneIndexValue(vh.getValueBySegment(i + 1), entryHolder);
            if (val != vh.getValueBySegment(i + 1))
                vh.setValueForSegment(val, i + 1);
        }
        return fieldValue;
    }

    @Override
    void updateValueType(Object fieldValue) {
        if (_valueTypeFinallySet)
            return;  //no need- the original indices has the type
        ICompoundIndexValueHolder h = (ICompoundIndexValueHolder) fieldValue;
        boolean finalTypes = true;
        for (int i = 0; i < _segments.length; i++) {
            if (!_segments[i].updateValueType(h.getValueBySegment(i + 1)))
                finalTypes = false;
        }
        if (finalTypes)
            _valueTypeFinallySet = true;
    }

}
