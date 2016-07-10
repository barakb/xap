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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ObjectShortMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Holds projection information per uid. several containted projections may be mapped by uids
 *
 * @author yechielf
 * @since 9.7
 */

@com.gigaspaces.api.InternalApi
public class MutliProjectionByUids extends AbstractProjectionTemplate {
    private static final long serialVersionUID = -3375363086422585765L;

    AbstractProjectionTemplate[] _projections;
    ObjectShortMap<String> _uidsByProjection;

    public MutliProjectionByUids(AbstractProjectionTemplate[] projections, ObjectShortMap<String> uidsByProjection) {
        super();
        _projections = projections;
        _uidsByProjection = uidsByProjection;
    }

    public MutliProjectionByUids() {
        super();
    }

    /**
     * given a entry-packet perform projection on it
     */
    @Override
    public void filterOutNonProjectionProperties(
            final IEntryPacket entryPacket) {
        if (!_uidsByProjection.containsKey(entryPacket.getUID()))
            return;
        short pos = _uidsByProjection.get(entryPacket.getUID());
        _projections[pos].filterOutNonProjectionProperties(entryPacket);
    }

    @Override
    public int[] getFixedPropertiesIndexes() {
        return null;
    }

    @Override
    public String[] getDynamicProperties() {
        return null;
    }

    @Override
    public String[] getFixedPaths() {
        return null;
    }

    @Override
    public String[] getDynamicPaths() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_projections);
        _uidsByProjection.serialize(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _projections = (AbstractProjectionTemplate[]) in.readObject();
        _uidsByProjection = CollectionsFactory.getInstance().deserializeObjectShortMap(in);

    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(_projections);
        result = prime * result + _uidsByProjection.hashCode();
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MutliProjectionByUids other = (MutliProjectionByUids) obj;
        if (!Arrays.equals(_projections, other._projections))
            return false;

        return _uidsByProjection.equals(other._uidsByProjection);
    }


}
