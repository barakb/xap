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

import com.gigaspaces.internal.utils.ObjectConverter;
import com.j_spaces.core.cache.CompoundCustomTypeDataIndex;
import com.j_spaces.core.cache.CompoundIndexSegmentTypeData;
import com.j_spaces.core.cache.CompoundIndexValueHolder;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.client.TemplateMatchCodes;

import net.jini.space.InternalSpaceException;

import java.sql.SQLException;

/**
 * @author Yechiel
 * @since 9.5
 */

@com.gigaspaces.api.InternalApi
public class CompoundConvertedObjectWrapper {

    Object _convertedObject;


    private CompoundConvertedObjectWrapper(Object value) {
        this._convertedObject = value;
    }

    public Object getValue() {
        return _convertedObject;
    }

    public static CompoundConvertedObjectWrapper create(Object[] segmentValues, TypeDataIndex<Object> index) {
        try {
            if (segmentValues == null)
                return new CompoundConvertedObjectWrapper(null);

            Object[] convertedSegments = new Object[segmentValues.length];
            for (int i = 0; i < segmentValues.length; i++) {
                convertedSegments[i] = CompoundConvertedObjectWrapper.convertSegmentValue(segmentValues[i], index.getCompoundIndexSegments()[i]);
                if (convertedSegments[i] == null)
                    return null;
            }
            return new CompoundConvertedObjectWrapper(new CompoundIndexValueHolder(convertedSegments));

        } catch (SQLException e) {
            throw new InternalSpaceException(e.getMessage(), e);
        }
    }

    public static CompoundConvertedObjectWrapper createForExtendedMatch(Object[] segmentValues, TypeDataIndex<Object> index, boolean isMin, boolean includeEdge, boolean includeEdgeFirstSegment) {
        try {
            Object convertedSeg1 = null;
            Object convertedSeg2 = null;

            if (segmentValues == null)
                return new CompoundConvertedObjectWrapper(null);

            Object[] convertedSegments = new Object[segmentValues.length];
            for (int i = 0; i < segmentValues.length; i++) {
                if (i > 0 && !includeEdgeFirstSegment) {
                    convertedSegments[i] = isMin ? CompoundCustomTypeDataIndex._highEdge : CompoundCustomTypeDataIndex._lowEdge;
                    continue;
                }
                convertedSegments[i] = CompoundConvertedObjectWrapper.convertSegmentValue(segmentValues[i], index.getCompoundIndexSegments()[i]);
                if (convertedSegments[i] == null)
                    return null;
            }
            short matchCode = isMin ? (includeEdge ? TemplateMatchCodes.GE : TemplateMatchCodes.GT) : (includeEdge ? TemplateMatchCodes.LE : TemplateMatchCodes.GT);
            return new CompoundConvertedObjectWrapper(new CompoundIndexValueHolder(convertedSegments));

        } catch (SQLException e) {
            throw new InternalSpaceException(e.getMessage(), e);
        }
    }


    private static Object convertSegmentValue(Object value, CompoundIndexSegmentTypeData segment)
            throws SQLException {
        // If value is null or not string, no need to convert:
        if (value == null || !(value instanceof String))
            return value;
        Class<?> type = segment.getValueType();

        // If target type is null it is impossible to convert:
        if (type == null)
            return null;

        return ObjectConverter.convert(value, type);
    }
}
