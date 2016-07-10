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

/*
 * @(#)LRMIRemoteEventListener 1.0   15/01/2006  9:50AM
 */

package com.gigaspaces.internal.lrmi.stubs;

import com.gigaspaces.annotation.lrmi.OneWayRemoteCall;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.ManagedRemoteEventListener;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.lrmi.OneWayMethodRepository;
import com.gigaspaces.lrmi.RemoteStub;
import com.j_spaces.kernel.SystemProperties;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;
import java.util.concurrent.Callable;

/**
 * Super class of all Notify stubs. <b>This the dynamic stub creates on the fly when user's
 * registers notify template</b>. No needs to extend from java.rmi.server.UnicastRemoteObject
 * working with LRMI layer(NIO). <p/> NOTE: OneWayRemoteCallAnnotation defines default Annotation
 * condition of one-way method call. By default RemoteEventListener.notify() method is one-way call.
 * <p/> To define one-way method
 *
 * @author Igor Goldenberg
 * @see com.gigaspaces.annotation.lrmi.OneWayRemoteCall
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class LRMIRemoteEventListener extends RemoteStub<RemoteEventListener> implements ManagedRemoteEventListener {
    static final long serialVersionUID = 2L;

    /** static initializer to register implicitly one-way notify() remote call invocation */
    static {
        init(new Callable() {
            public Object call() throws SecurityException, NoSuchMethodException {
                String oneWayNotifySysProp = System.getProperty(SystemProperties.ONE_WAY_NOTIFY, Boolean.TRUE.toString());
                boolean oneWayNotifyRequired = Boolean.parseBoolean(oneWayNotifySysProp);
                if (!oneWayNotifyRequired)
                    return null;
                IMethod[] methods = ReflectionUtil.createMethods(LRMIRemoteEventListener.class.getDeclaredMethods());
                for (IMethod m : methods) {
                    OneWayRemoteCall oneWayAnnotation = m.getAnnotation(OneWayRemoteCall.class);
                    if (oneWayAnnotation == null)
                        continue;

                    m = ReflectionUtil.createMethod(RemoteEventListener.class.getMethod(m.getName(), m.getParameterTypes()));
                    OneWayMethodRepository.register(m);
                }// for

                return null;
            }
        });
    }

    // NOTE, here just for externalizable
    public LRMIRemoteEventListener() {
    }

    /**
     * constructor to initialize notify remote event listener
     */
    public LRMIRemoteEventListener(RemoteEventListener directRefProxy, RemoteEventListener dynamicProxy) {
        super(directRefProxy, dynamicProxy);
    }

    @Override
    public void shutdown(GSEventRegistration registration) {
        RemoteEventListener proxy = getProxy();
        if (proxy instanceof ManagedRemoteEventListener)
            ((ManagedRemoteEventListener) proxy).shutdown(registration);
    }

    @OneWayRemoteCall
    public void notify(RemoteEvent event) throws RemoteException, UnknownEventException {
        getProxy().notify(event);
    }


    @Override
    public void init(GSEventRegistration registration) {
        RemoteEventListener listener = getProxy();
        if (listener instanceof ManagedRemoteEventListener)
            ((ManagedRemoteEventListener) listener).init(registration);
    }
}// end LRMINotifyListener class