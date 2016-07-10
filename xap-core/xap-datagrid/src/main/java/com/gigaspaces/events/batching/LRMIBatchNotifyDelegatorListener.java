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

package com.gigaspaces.events.batching;

import com.gigaspaces.annotation.lrmi.OneWayRemoteCall;
import com.gigaspaces.internal.lrmi.stubs.LRMINotifyDelegatorListener;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.lrmi.OneWayMethodRepository;
import com.j_spaces.kernel.SystemProperties;

import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;
import java.util.concurrent.Callable;

/**
 * Notify delegator for batch notifications.
 */
@com.gigaspaces.api.InternalApi
public class LRMIBatchNotifyDelegatorListener extends LRMINotifyDelegatorListener implements BatchRemoteEventListener {
    private static final long serialVersionUID = 5410444772526906875L;

    /** static initializer to register implicitly one-way notify() remote call invocation */
    static {
        init(new Callable() {
            public Object call() throws SecurityException, NoSuchMethodException {
                String oneWayNotifySysProp = System.getProperty(SystemProperties.ONE_WAY_NOTIFY, Boolean.TRUE.toString());
                boolean oneWayNotifyRequired = Boolean.parseBoolean(oneWayNotifySysProp);
                if (!oneWayNotifyRequired)
                    return null;
                IMethod[] methods = ReflectionUtil.createMethods(LRMIBatchNotifyDelegatorListener.class.getDeclaredMethods());
                for (IMethod m : methods) {
                    OneWayRemoteCall oneWayAnnotation = m.getAnnotation(OneWayRemoteCall.class);
                    if (oneWayAnnotation == null)
                        continue;

                    m = ReflectionUtil.createMethod(BatchRemoteEventListener.class.getMethod(m.getName(), m.getParameterTypes()));
                    OneWayMethodRepository.register(m);
                }// for

                return null;
            }
        });
    }

    // NOTE, here just for externalizable
    public LRMIBatchNotifyDelegatorListener() {
    }

    /**
     * constructor to initialize NotifyDelegator remoteEventListener
     */
    public LRMIBatchNotifyDelegatorListener(BatchRemoteEventListener directObjRef, BatchRemoteEventListener dynamicProxy) {
        super(directObjRef, dynamicProxy);
    }

    @OneWayRemoteCall
    public void notifyBatch(BatchRemoteEvent theEvents) throws UnknownEventException, RemoteException {
        ((BatchRemoteEventListener) getProxy()).notifyBatch(theEvents);
    }
}
