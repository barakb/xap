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

package com.gigaspaces.metadata.index;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.serialization.AbstractExternalizable;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * <p> Encapsulates information about an index segment of a compound index.
 *
 * @author Yechiel Fefer
 * @since 9.5
 */
public abstract class AbstractCompoundIndexSegment extends AbstractExternalizable implements ISpaceCompoundIndexSegment {
    private static final long serialVersionUID = 1L;

    private int _segmentPos;
    private String _path;

    public AbstractCompoundIndexSegment() {
    }

    public AbstractCompoundIndexSegment(int segmentPos, String path) {
        _segmentPos = segmentPos;
        _path = path;
    }

    /**
     * @return the value that will be used for this segment using a given entry
     */
    public abstract Object getSegmentValue(ServerEntry entry);

    /**
     * @return the value that will be used for this segment using a given template
     */
    public Object getSegmentValueForTemplate(ServerEntry entry) {
        return null;   //we dont support templates compound indexing
    }


    /**
     * @return the positio0n of this segment in the compound index
     */
    public int getSegmentPosition() {
        return _segmentPos;
    }


    /**
     * @return true if its a property segment
     */
    @Override
    public boolean isPropertySegment() {
        return false;
    }

    /**
     * @return the path name of this segment
     */
    public String getName() {
        return _path;
    }


    @Override
    public int hashCode() {
        return _path.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractCompoundIndexSegment other = (AbstractCompoundIndexSegment) obj;
        return other._segmentPos == _segmentPos && other._path.equals(_path);
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    private final void deserialize(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _segmentPos = in.readInt();
        _path = IOUtils.readString(in);
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        serialize(out);
    }

    private final void serialize(ObjectOutput out) throws IOException {
        out.writeInt(_segmentPos);
        IOUtils.writeString(out, _path);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }


}
