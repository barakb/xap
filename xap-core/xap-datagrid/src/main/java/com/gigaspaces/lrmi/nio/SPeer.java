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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.lrmi.BaseServerPeer;

import java.rmi.RemoteException;


/**
 * SPeer is the LRMI over NIO Server Peer.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class SPeer
        extends BaseServerPeer {
    public SPeer(PAdapter pAdapter, long objectId, ClassLoader objectClassLoader, String serviceDetails) {
        super(pAdapter, objectId, objectClassLoader, serviceDetails);
    }

    @Override
    public void afterUnexport(boolean force) throws RemoteException {
        super.afterUnexport(force);

		/* forcibly unexport connections of supplied remote objectID */
        getProtocolAdapter().getPivot().unexport(getObjectId());
    }
}