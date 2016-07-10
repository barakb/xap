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

package com.gigaspaces.internal.cluster.node.impl.packets.data.errors;

import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeErrorResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataProducer;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.j_spaces.core.UnknownTypeException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * A consume error result caused due to {@link UnknownTypeException} thrown during consumption
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class UnknownTypeConsumeResult
        extends AbstractDataConsumeErrorResult {
    private static final long serialVersionUID = 1L;
    private String _unknownTypeClassName;
    private transient UnknownTypeException _exception;

    public UnknownTypeConsumeResult() {
    }

    public UnknownTypeConsumeResult(String unknownTypeClassName,
                                    UnknownTypeException exception) {
        this._unknownTypeClassName = unknownTypeClassName;
        _exception = exception;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _unknownTypeClassName = IOUtils.readString(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _unknownTypeClassName);
    }

    @Override
    public AbstractDataConsumeFix createFix(SpaceEngine spaceEngine,
                                            ReplicationPacketDataProducer producer, IExecutableReplicationPacketData errorData) {
        ITypeDesc typeDescriptor = spaceEngine.getTypeManager()
                .getTypeDesc(_unknownTypeClassName);

        return new TypeDescriptorConsumeFix(typeDescriptor);
    }

    public Exception toException() {
        return _exception;
    }

    public boolean sameFailure(IDataConsumeResult otherResult) {
        if (!(otherResult instanceof UnknownTypeConsumeResult))
            return false;

        return ((UnknownTypeConsumeResult) otherResult)._unknownTypeClassName.equals(_unknownTypeClassName);
    }
}
