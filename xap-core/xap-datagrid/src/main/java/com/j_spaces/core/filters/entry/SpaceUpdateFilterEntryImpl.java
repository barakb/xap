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

package com.j_spaces.core.filters.entry;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.AbstractEntryType;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.NotifyModifiers;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;

import java.rmi.MarshalledObject;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class SpaceUpdateFilterEntryImpl extends AbstractEntryType implements ISpaceFilterEntry {
    private static final long serialVersionUID = 8613812565006569044L;

    protected final IEntryPacket _entryPacket;

    public SpaceUpdateFilterEntryImpl(IEntryPacket entryPacket, ITypeDesc typeDesc) {
        super(typeDesc);
        this._entryPacket = entryPacket;
    }

    /*
     * @see com.j_spaces.core.filters.entry.IFilterEntry#getHandback()
     */
    public MarshalledObject getHandback() {
        return null;
    }

    /*
     * @see com.j_spaces.core.filters.entry.IFilterEntry#getNotifyType()
     */
    public int getNotifyType() {
        return NotifyModifiers.NOTIFY_NONE;
    }

    public Map.Entry getMapEntry() {
        return null;
    }

    public String getUID() {
        return _entryPacket.getUID();
    }

    @Override
    public String getClassName() {
        return _entryPacket.getTypeName();
    }

    public Object[] getFieldsValues() {
        return _entryPacket.getFieldValues();
    }

    public boolean isTransient() {
        return _entryPacket.isTransient();
    }

    public long getTimeToLive() {
        return _entryPacket.getTTL();
    }

    public int getVersion() {
        return _entryPacket.getVersion();
    }

    public Object getFieldValue(String fieldName) {
        return _entryPacket.getPropertyValue(fieldName);
    }

    public Object getFieldValue(int position) {
        return _entryPacket.getFieldValue(position);
    }

    public Object setFieldValue(String fieldName, Object value) {
        Object old = _entryPacket.getPropertyValue(fieldName);
        _entryPacket.setPropertyValue(fieldName, value);
        return old;
    }

    public Object setFieldValue(int position, Object value) {
        Object old = _entryPacket.getFieldValue(position);
        _entryPacket.setFieldValue(position, value);
        return old;
    }

    public Entry getEntry(IJSpace space) throws UnusableEntryException {
        return (Entry) getObject(space);
    }
}
