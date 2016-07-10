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

package com.gigaspaces.internal.service;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.security.service.SecuredService;

import net.jini.core.lookup.ServiceItem;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class ServiceItemUtils {
    public static SecuredService getSecuredService(ServiceItem serviceItem) {
        return serviceItem != null && serviceItem.service != null ? (SecuredService) serviceItem.service : null;
    }

    public static SecuredService getSecuredServiceIfRelevant(ServiceItem serviceItem) {
        return serviceItem != null && serviceItem.service instanceof SecuredService ? (SecuredService) serviceItem.service : null;
    }

    public static IDirectSpaceProxy getSpaceProxy(ServiceItem serviceItem) {
        return serviceItem != null && serviceItem.service != null ? (IDirectSpaceProxy) serviceItem.service : null;
    }

    public static IDirectSpaceProxy getSpaceProxyIfRelevant(ServiceItem serviceItem) {
        return serviceItem != null && serviceItem.service instanceof IDirectSpaceProxy ? (IDirectSpaceProxy) serviceItem.service : null;
    }

    public static String getSpaceMemberName(ServiceItem serviceItem) {
        IDirectSpaceProxy spaceProxy = getSpaceProxy(serviceItem);
        return spaceProxy == null ? null : spaceProxy.getRemoteMemberName();
    }
}
