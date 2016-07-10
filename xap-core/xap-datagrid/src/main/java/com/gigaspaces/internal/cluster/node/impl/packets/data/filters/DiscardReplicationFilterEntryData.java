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

package com.gigaspaces.internal.cluster.node.impl.packets.data.filters;

import com.j_spaces.core.AbstractEntryType;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.rmi.MarshalledObject;
import java.util.Map.Entry;

@com.gigaspaces.api.InternalApi
public class DiscardReplicationFilterEntryData extends AbstractEntryType implements IReplicationFilterEntry {

    private static final long serialVersionUID = 1L;

    public static final DiscardReplicationFilterEntryData ENTRY = new DiscardReplicationFilterEntryData();

    public DiscardReplicationFilterEntryData() {
        super(null);
    }


    public void discard() {
    }

    public int getObjectType() {
        return ObjectTypes.ENTRY;
    }

    public ReplicationOperationType getOperationType() {
        return ReplicationOperationType.DISCARD;
    }

    public boolean isDiscarded() {
        return true;
    }

    public void setFieldsValues(Object[] values) {
        throw new UnsupportedOperationException();
    }

    public void setTimeToLive(long time) {
    }

    public MarshalledObject getHandback() {
        return null;
    }

    public int getNotifyType() {
        return 0;
    }

    public Object getFieldValue(int position) throws IllegalArgumentException,
            IllegalStateException {
        throw new UnsupportedOperationException();
    }

    public Object[] getFieldsValues() {
        return null;
    }

    public Entry getMapEntry() {
        return null;
    }

    public long getTimeToLive() {
        return 0;
    }

    public String getUID() {
        return null;
    }

    public int getVersion() {
        return 0;
    }

    public boolean isTransient() {
        return false;
    }

    public Object setFieldValue(String fieldName, Object value)
            throws IllegalArgumentException, IllegalStateException {
        throw new UnsupportedOperationException();
    }

    public Object setFieldValue(int position, Object value)
            throws IllegalArgumentException, IllegalStateException {
        throw new UnsupportedOperationException();
    }


    public Object getFieldValue(String fieldName)
            throws IllegalArgumentException, IllegalStateException {
        throw new UnsupportedOperationException();
    }
}
