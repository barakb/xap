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

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.reflection.ReflectionUtil;

import java.lang.reflect.Proxy;


/**
 * The Transport Protocol utility class.
 *
 * @author Igor Goldenberg
 * @since 6.0
 **/
@com.gigaspaces.api.InternalApi
public class TransportProtocolHelper {
    /**
     * Returns remoteObjID of supplied dynamic smartStub.
     *
     * @param obj the dynamic smartStub represents remoteObj.
     * @return remoteObjID of this dynamic smartStub or -1 if supplied is not DynamicSmartStub.
     **/
    static public long getRemoteObjID(Object obj) {
        DynamicSmartStub smartStub = extractSmartStubFromProxy(obj);
        if (smartStub != null)
            return smartStub.getRemoteObjID();

	  /* the supplied object is not dynamicSmartStub */
        return -1;
    }

    /**
     * Extract DynamicSmartStub instance from desired dynamic proxy.
     *
     * @param dynamicSmartProxy the dynamicProxy created by {@link Proxy}.
     * @return the extracted DynamicSmartStub wrapped by {@link Proxy}.
     **/
    public static DynamicSmartStub extractSmartStubFromProxy(Object dynamicProxy) {
        if (dynamicProxy == null)
            throw new IllegalArgumentException("The supplied dynamicProxy can not be null.");

	   /* extract dynamic proxy from RemoteStub */
        if (dynamicProxy instanceof RemoteStub)
            dynamicProxy = ((RemoteStub) dynamicProxy).getProxy();

        if (ReflectionUtil.isProxyClass(dynamicProxy.getClass())) {
            Object proxyObj = ReflectionUtil.getInvocationHandler(dynamicProxy);
            if (proxyObj instanceof DynamicSmartStub)
                return (DynamicSmartStub) proxyObj;
        }

	   /* not a dynamic proxy */
        return null;
    }

    /**
     * Checks whether supplied obj is DynamicSmartStub wrapped by {@link Proxy} or {@link
     * RemoteStub}.
     *
     * @param obj dynamic proxy.
     * @return <code>true</code> if the supplied object is dynamicSmartStub.
     **/
    public static boolean isDynamicSmartStub(Object obj) {
        return getRemoteObjID(obj) != -1;
    }
}
