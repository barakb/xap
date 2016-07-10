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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * The element of synchronizing a direct-persistency uid
 *
 * @author yechielf
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencySingleUidOpInfo extends AbstractDirectPersistencyOpInfo {

    private static final long serialVersionUID = 1L;
    private String _uid;


    public DirectPersistencySingleUidOpInfo(long generationId, String uid, int seq1, int seq2) {
        super(generationId, seq1, seq2);
        _uid = uid;
    }

    public DirectPersistencySingleUidOpInfo(long generationId, String uid, long seq) {
        super(generationId, seq);
        _uid = uid;
    }

    public DirectPersistencySingleUidOpInfo() {
        super();
    }

    @Override
    public String getUid() {
        return _uid;
    }

    @Override
    public boolean isMultiUids() {
        return false;

    }


    @Override
    public List<String> getUids() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(_uid);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _uid = in.readUTF();
    }


}
