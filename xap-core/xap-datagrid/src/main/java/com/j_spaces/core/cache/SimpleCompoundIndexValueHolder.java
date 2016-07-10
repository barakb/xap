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
 * Holder for simple compound index value of 2 segments
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 9.0
 */


@com.gigaspaces.api.InternalApi
public class SimpleCompoundIndexValueHolder
        implements ICompoundIndexValueHolder {

    private Object _val1;
    private Object _val2;


    public SimpleCompoundIndexValueHolder(ISpaceCompoundIndexSegment[] segments, ServerEntry entry) {
        _val1 = segments[0].getSegmentValue(entry);
        _val2 = segments[1].getSegmentValue(entry);
    }

    public SimpleCompoundIndexValueHolder(Object val1, Object val2) {
        _val1 = val1;
        _val2 = val2;
    }

    @Override
    public Object getValueBySegment(int segmentNumber) {
        // TODO Auto-generated method stub
        if (segmentNumber != 1 && segmentNumber != 2)
            throw new InvalidParameterException();
        return segmentNumber == 1 ? _val1 : _val2;
    }

    @Override
    public int getNumSegments() {
        return 2;
    }

    public void setValueForSegment(Object value, int segmentNumber) {
        if (segmentNumber > 2 || segmentNumber < 1)
            throw new IllegalArgumentException();
        if (segmentNumber == 1)
            _val1 = value;
        else
            _val2 = value;
    }


    @Override
    public int hashCode() {
        int hash = 17;
        if (_val1 != null)
            hash = hash * 31 + _val1.hashCode();
        if (_val2 != null)
            hash = hash * 31 + _val2.hashCode();

        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;
        if (!(other instanceof ICompoundIndexValueHolder))
            return false;
        ICompoundIndexValueHolder o = (ICompoundIndexValueHolder) other;
        return _val1.equals(o.getValueBySegment(1)) && _val2.equals(o.getValueBySegment(2));
    }

    @Override
    public int compareTo(ICompoundIndexValueHolder o) {
        if (o == this)
            return 0;

        int res = 0;
        if (o.getValueBySegment(1) != _val1) {
            Comparable v1 = (Comparable) _val1;
            Comparable v2 = (Comparable) o.getValueBySegment(1);
            if ((res = v1.compareTo(v2)) != 0)
                return res;
        }
        if (o.getValueBySegment(2) != _val2) {
            Comparable v1 = (Comparable) _val2;
            Comparable v2 = (Comparable) o.getValueBySegment(2);
            res = v1.compareTo(v2);
        }
        return res;
    }

    public Object[] getValues() {
        return new Object[]{_val1, _val2};
    }

}
