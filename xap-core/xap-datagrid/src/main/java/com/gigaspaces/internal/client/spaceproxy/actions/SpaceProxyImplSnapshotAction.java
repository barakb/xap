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

package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.SnapshotProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.PropertiesUtils;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.Constants;
import com.j_spaces.core.client.EntrySnapshot;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.entry.UnusableEntryException;
import net.jini.space.InternalSpaceException;

import java.rmi.RemoteException;
import java.util.Properties;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplSnapshotAction extends SnapshotProxyAction<SpaceProxyImpl> {
    @Override
    public <T> ISpaceQuery<T> snapshot(SpaceProxyImpl spaceProxy, SnapshotProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException {
        final long lookupTimeout = getLookupTimeout(spaceProxy);
        final long timeToEnd = SystemTime.timeMillis() + lookupTimeout;

        IRemoteSpace rj = spaceProxy.getRemoteJSpace();
        while (true) {
            try {
                ITemplatePacket queryPacket = actionInfo.queryPacket;
                if (actionInfo.isSqlQuery) {
                    queryPacket = spaceProxy.getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) actionInfo.queryPacket, null);
                    // fill the SQLQuery generated template with the metadata
                    queryPacket = spaceProxy.getTypeManager().getTemplatePacketFromObject(queryPacket, ObjectType.TEMPLATE_PACKET);
                    queryPacket.setSerializeTypeDesc(true);
                }

                if (queryPacket == null)
                    throw new IllegalArgumentException("Cannot accept null Entry/Template.");

                spaceProxy.beforeSpaceAction();

                try {
                    rj.snapshot(queryPacket);
                    queryPacket.setSerializeTypeDesc(false);
                    return new EntrySnapshot(queryPacket);
                } catch (UnusableEntryException ex) {
                    throw new InternalSpaceException(ex.toString(), ex);
                }
            } catch (RemoteException ex) {
                if (SystemTime.timeMillis() >= timeToEnd)
                    break;

                rj = spaceProxy.getProxyRouter().getAnyAvailableSpace();
                if (rj == null)
                    break;
            }
        }

        throw new RemoteException("Can not locate an available space...");
    }

    private long getLookupTimeout(SpaceProxyImpl spaceProxy) {
        Properties properties = spaceProxy.getProxySettings().getCustomProperties();
        return PropertiesUtils.getLong(properties, Constants.SpaceProxy.Router.ACTIVE_SERVER_LOOKUP_TIMEOUT,
                Constants.SpaceProxy.Router.ACTIVE_SERVER_LOOKUP_TIMEOUT_DEFAULT);
    }
}
