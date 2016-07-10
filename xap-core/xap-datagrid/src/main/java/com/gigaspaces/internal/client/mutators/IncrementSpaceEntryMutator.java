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

package com.gigaspaces.internal.client.mutators;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.server.MutableServerEntry;
import com.gigaspaces.sync.change.IncrementOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author Niv Ingberg
 * @since 9.1
 */
public final class IncrementSpaceEntryMutator extends SpaceEntryPathMutator {
    private static final long serialVersionUID = 1L;

    private Number _delta;

    public IncrementSpaceEntryMutator() {
    }

    public IncrementSpaceEntryMutator(String path, Number delta) {
        super(path);
        if (delta == null)
            throw new IllegalArgumentException("delta cannot be null");
        _delta = delta;
    }

    @Override
    public Object change(MutableServerEntry entry) {
        Number oldValue = (Number) entry.getPathValue(getPath());
        Number newValue = _delta;
        if (oldValue != null && !oldValue.getClass().equals(newValue.getClass()))
            throw new IllegalArgumentException("attempting to increment old value [" + oldValue + "," + oldValue.getClass() + "] with incompatible number type [" + newValue + "," + newValue.getClass() + "]");
        if (oldValue instanceof Byte)
            newValue = oldValue.byteValue() + _delta.byteValue();
        else if (oldValue instanceof Short)
            newValue = oldValue.shortValue() + _delta.shortValue();
        else if (oldValue instanceof Integer)
            newValue = oldValue.intValue() + _delta.intValue();
        else if (oldValue instanceof Long)
            newValue = oldValue.longValue() + _delta.longValue();
        else if (oldValue instanceof Float)
            newValue = oldValue.floatValue() + _delta.floatValue();
        else if (oldValue instanceof Double)
            newValue = oldValue.doubleValue() + _delta.doubleValue();
        else if (oldValue instanceof BigInteger)
            newValue = ((BigInteger) oldValue).add((BigInteger) newValue);
        else if (oldValue instanceof BigDecimal)
            newValue = ((BigDecimal) oldValue).add((BigDecimal) newValue);
        else if (oldValue != null)
            throw new IllegalStateException("old value is of unknown number type - " + oldValue.getClass());

        entry.setPathValue(getPath(), newValue);

        return newValue;
    }

    private static final byte FLAG_BYTE = 1 << 0;
    private static final byte FLAG_SHORT = 1 << 1;
    private static final byte FLAG_INT = 1 << 2;
    private static final byte FLAG_LONG = 1 << 3;
    private static final byte FLAG_FLOAT = 1 << 4;
    private static final byte FLAG_DOUBLE = 1 << 5;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            IOUtils.writeObject(out, _delta);
        else {
            if (_delta instanceof Integer) {
                out.writeByte(FLAG_INT);
                out.writeInt(_delta.intValue());
            } else if (_delta instanceof Long) {
                out.writeByte(FLAG_LONG);
                out.writeLong(_delta.longValue());
            } else if (_delta instanceof Double) {
                out.writeByte(FLAG_DOUBLE);
                out.writeDouble(_delta.doubleValue());
            } else if (_delta instanceof Float) {
                out.writeByte(FLAG_FLOAT);
                out.writeFloat(_delta.floatValue());
            } else if (_delta instanceof Byte) {
                out.writeByte(FLAG_BYTE);
                out.writeByte(_delta.byteValue());
            } else if (_delta instanceof Short) {
                out.writeByte(FLAG_SHORT);
                out.writeShort(_delta.shortValue());
            } else
                throw new UnsupportedOperationException("Illegal delta type - " + _delta.getClass());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            _delta = IOUtils.readObject(in);
        else {
            byte flag = in.readByte();
            if ((flag & FLAG_BYTE) != 0)
                _delta = in.readByte();
            if ((flag & FLAG_SHORT) != 0)
                _delta = in.readShort();
            if ((flag & FLAG_INT) != 0)
                _delta = in.readInt();
            if ((flag & FLAG_LONG) != 0)
                _delta = in.readLong();
            if ((flag & FLAG_FLOAT) != 0)
                _delta = in.readFloat();
            if ((flag & FLAG_DOUBLE) != 0)
                _delta = in.readDouble();
        }
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("delta", _delta);
    }

    @Override
    public String getName() {
        return IncrementOperation.NAME;
    }

    public Number getDelta() {
        return _delta;
    }
}
