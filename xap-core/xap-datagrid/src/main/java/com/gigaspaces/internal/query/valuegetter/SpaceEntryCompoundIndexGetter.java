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

package com.gigaspaces.internal.query.valuegetter;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.metadata.index.ISpaceCompoundIndexSegment;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CompoundIndexValueHolder;
import com.j_spaces.core.cache.SimpleCompoundIndexValueHolder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * A space value getter for getting a compound index value from a space entry.
 *
 * @author Yechiel Fefer
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceEntryCompoundIndexGetter extends AbstractSpaceValueGetter<ServerEntry> {
    private static final long serialVersionUID = -9025377890997556363L;

    private ISpaceCompoundIndexSegment[] _segments;

    /**
     * Default constructor required by Externalizable.
     */
    public SpaceEntryCompoundIndexGetter() {
    }

    public ISpaceCompoundIndexSegment[] getSegments() {
        return _segments;
    }

    /**
     * Create a compound  getter using the specified segments.
     */
    public SpaceEntryCompoundIndexGetter(ISpaceCompoundIndexSegment[] segments) {
        _segments = segments;
    }

    @Override
    public Object getValue(ServerEntry target) {
        //currently we dont support null segment
        if (anyNullSegment(target))
            return null;
        return _segments.length == 2 ? new SimpleCompoundIndexValueHolder(_segments, target) :
                new CompoundIndexValueHolder(_segments, target);
    }

    public boolean anyNullSegment(ServerEntry entry) {
        if (_segments.length == 2) {
            return _segments[0].getSegmentValue(entry) == null ||
                    _segments[1].getSegmentValue(entry) == null;
        } else {
            for (ISpaceCompoundIndexSegment segment : _segments) {
                if (segment.getSegmentValue(entry) == null)
                    return true;
            }
            return false;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !this.getClass().equals(obj.getClass()))
            return false;

        SpaceEntryCompoundIndexGetter other = (SpaceEntryCompoundIndexGetter) obj;
        return (Arrays.deepEquals(_segments, other._segments));

    }

    @Override
    public int hashCode() {
        return _segments[0].hashCode();
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);

        this._segments = IOUtils.readObject(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);

        IOUtils.writeObject(out, _segments);
    }


    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        readExternalImpl(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

}
