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

import com.gigaspaces.metadata.index.ISpaceCompoundIndexSegment;
import com.gigaspaces.server.ServerEntry;

import java.security.InvalidParameterException;

/**
 * Holder for compound index value of  > 2 segments
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */

@com.gigaspaces.api.InternalApi
public class CompoundIndexValueHolder
        implements ICompoundIndexValueHolder

{
    private final Object[] _values;

    public CompoundIndexValueHolder(ISpaceCompoundIndexSegment[] segments, ServerEntry entry) {
        _values = new Object[segments.length];
        for (int i = 0; i < segments.length; i++)
            _values[i] = segments[i].getSegmentValue(entry);
    }

    public CompoundIndexValueHolder(Object[] segmentValues) {
        _values = segmentValues;
    }

    @Override
    public Object getValueBySegment(int segmentNumber) {
        // TODO Auto-generated method stub
        if (segmentNumber < 1 || segmentNumber > _values.length)
            throw new InvalidParameterException();
        return _values[segmentNumber - 1];
    }

    @Override
    public int getNumSegments() {
        return _values.length;
    }

    public void setValueForSegment(Object value, int segmentNumber) {
        if (segmentNumber > _values.length || segmentNumber < 1)
            throw new IllegalArgumentException();
        _values[segmentNumber - 1] = value;
    }

    //	@Override
    public boolean isExtendedMatchConditionHolder() {
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 17;

        for (int i = 0; i < _values.length; i++) {
            if (_values[i] != null)
                hash = hash * 31 + _values[i].hashCode();
        }

        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;
        if (!(other instanceof ICompoundIndexValueHolder))
            return false;
        ICompoundIndexValueHolder o = (ICompoundIndexValueHolder) other;

        if (_values.length != o.getNumSegments())
            return false;
        for (int i = 0; i < _values.length; i++)
            if (!_values[i].equals(o.getValueBySegment(i + 1)))
                return false;

        return true;
    }

    @Override
    public int compareTo(ICompoundIndexValueHolder o) {
        if (o == this)
            return 0;

        int res = 0;
        for (int i = 0; i < _values.length; i++) {
            if (o.getValueBySegment(i + 1) == _values[i])
                continue;
            Comparable v1 = (Comparable) _values[i];
            Comparable v2 = (Comparable) o.getValueBySegment(i + 1);
            if ((res = v1.compareTo(v2)) != 0)
                return res;
        }
        return 0;
    }

    public Object[] getValues() {
        return _values;
    }

}
