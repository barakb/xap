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
 * @(#)RemoteJavaProcess.java   Jul 22, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

/**
 * RemoteJavaProcess provides server peer of {@link RemoteJavaCommand} and manages the lifecycle of
 * forked JVM.<br> All remote methods received from client peer {@link RemoteJavaCommand} will be
 * invoked on user-defined impl-class {@link #getTargetInvObject()}.<br> {@link RemoteJavaProcess}
 * inherits WatchDog capabilities from JavaProcess, if parent JVM was destroyed, the
 * RemoteJavaProcess will be destroyed as well by WatchDog.
 *
 * @author Igor Goldenberg
 * @see RemoteJavaCommand
 * @see IRemoteJavaProcessAdmin
 * @since 1.0
 **/
public class RemoteJavaProcess
        extends JavaProcess
        implements InternalRemoteJavaProcessProtocol {
    /**
     * target invocation object
     */
    private final Object _targetInvObj;

    /**
     * Constructor.
     */
    @SuppressWarnings("rawtypes")
    public RemoteJavaProcess(String[] args) throws ForkProcessException {
        super(args);

        try {
            Class claz = Class.forName(getProcessArgs().getMainClass());
            _targetInvObj = claz.newInstance();
        } catch (Exception e) {
            throw newForkProcessException(e);
        }

    }

    /**
     * @return an instance of user-defined class
     */
    protected Object getTargetInvObject() {
        return _targetInvObj;
    }


    /**
     * Received remote method from client peer {@link RemoteJavaCommand} to invoke on {@link
     * #getTargetInvObject()} object.
     *
     * @see java.lang.reflect.InternalRemoteJavaProcessProtocol#invoke(java.lang.Object,
     * java.lang.reflect.Method, java.lang.Object[])
     */
    @SuppressWarnings("rawtypes")
    public Object invoke(String methodName, Class[] paramType, Object[] args)
            throws RemoteException, InvocationTargetException {
        try {
            Method invMethod = getTargetInvObject().getClass().getMethod(methodName, paramType);
            return invMethod.invoke(getTargetInvObject(), args);
        } catch (InvocationTargetException e) {
            throw e;
        } catch (Throwable tr) {
            throw new RemoteException("Failed to invoke: [" + methodName + "] on class: [" + getTargetInvObject().getClass().getName() + "]", tr);
        }
    }

    /**
     * @see org.openspaces.test.client.executor.IRemoteJavaProcessAdmin#isAlive()
     */
    public boolean isAlive() throws RemoteException, ForkProcessException {
        return true;
    }

    /**
     * @see org.openspaces.test.client.executor.IRemoteJavaProcessAdmin#killVM()
     */
    public void killVM() throws RemoteException {
        //killVM in a separate thread, don't block invoker thread 
        new Thread(new Runnable() {
            public void run() {
                try {
                    /* release agent connection and after 500 ms killVM */
                    Thread.sleep(500);
                    Runtime.getRuntime().halt(1);
                } catch (InterruptedException e) {
                }
            }
        }).start();
    }

    public void dumpVM() throws RemoteException {
        Object targetInvObject = getTargetInvObject();
        if (targetInvObject instanceof IRemoteDumpProvider) {
            IRemoteDumpProvider dumpProvider = (IRemoteDumpProvider) targetInvObject;
            try {
                dumpProvider.generateDump();
            } catch (Exception e) {
                ProcessLogger.log("Failed to dump JVM on " + targetInvObject, e);
            }
        } else {
            ProcessLogger.log("Skipping dump for [" + targetInvObject + "] - not instanceof " + IRemoteDumpProvider.class.getName());
        }
    }

    /**
     * binds the process to the registry. <p> Note: doesn't call the process main method! Only the
     * public no-args constructor is called, as would be expected from a regular class invoked
     * remotely.
     */
    @Override
    protected void run() throws Exception {
        bindToProcessRegistry();
    }

    /**
     * the main method to initialize {@link RemoteJavaProcess}
     */
    public static void main(String... args) {
        System.setErr(System.out);

        ProcessLogger.log("RemoteJavaProcess - Starting with args: " + Arrays.asList(args));

        try {
            RemoteJavaProcess jp = new RemoteJavaProcess(args);
            jp.run();

            /*
             * need to hang process so that we can perform remote interactions
             * with it's interface. Only when requested, the process is
             * terminated.
             */
            Thread.sleep(1000 * 60 * 60 * 60); // max 24 hours
        } catch (Throwable t) {
            ProcessLogger.log("RemoteJavaProcess - Termination was caused by: ", t);
            System.exit(1);
        }
    }
    
    
    /*
     * RMI handling
     */

    /**
     * Returns stub of this class. This stub will be registered to RMIRegistry of {@link
     * RemoteJavaCommand}.
     *
     * @throws RemoteException Failed to export {@link RemoteJavaProcess} to stub.
     * @see org.openspaces.test.client.executor.JavaProcess#getProcessStub(java.rmi.registry.Registry,
     * java.lang.String)
     */
    @Override
    protected Remote getProcessStub() throws RemoteException {
        return UnicastRemoteObject.exportObject(this, ExecutorUtils.getAnonymousPort());
    }
}
