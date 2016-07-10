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
 * @(#)RemoteJavaCommand.java   Jul 22, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.rmi.ConnectException;
import java.rmi.Remote;
import java.util.Arrays;
import java.util.Map;

/**
 * The RemoteJavaCommand provides ability to control forkable process as remote service, all
 * communication with forked JVM process performs via RMI.<br> The example below shows which steps
 * need to do to define {@link RemoteJavaCommand}. <li>1. Define remote interface with remote
 * methods. <li>2. Create class which implements the remote interface with <u>optional</u> main()
 * method. <li>3. Instantiate RemoteJavaCommand with appropriate remote interface, impl-class,
 * <u>optional</u>: jvm-args/sys-prop/main-args. <li>4. Call {@link Executor#executeAsync(RemoteJavaCommand,
 * java.io.File)} <li>5. The return value Executor.executeAsync() is {@link
 * RemoteAsyncCommandResult} provides access {@link RemoteAsyncCommandResult#getRemoteStub()} to
 * remote process. getRemoteStub() returns the user-defined interface and direct reference to forked
 * process. <li>6. Now when you call echoStub.sayEcho(), it invokes remote call on forked process.
 * <li>7. Stop/Destroy process by {@link RemoteAsyncCommandResult#stop(boolean)} <p> Example:
 * <pre>
 * 1.
 * public interface IRemoteEcho extends java.rmi.Remote
 * {
 *   public String sayEcho( String name ) throws java.rmi.RemoteException;
 * }
 *
 * 2.
 * public class RemoteEchoImpl implements IRemoteEcho
 * {
 *   public String sayEcho( String name ) throws java.rmi.RemoteException
 *   {
 *     return "Hello: " + name;
 *   }
 * }
 *
 * 3.
 * ClassPath classpath = new ClassPath("echo.jar");
 * RemoteJavaCommand<IRemoteEcho> rjavaCmd = new RemoteJavaCommand<IRemoteSpace>(RemoteEchoImpl.class,
 * classpath);
 * rjavaCmd.addJVMArg("-Xmx128m");
 * rjavaCmd.addSystemPropParameter("my.echo.property", "echo-value");
 * rjavaCmd.addMainArgs("arg1", "arg2", "arg3");
 * rjavaCmd.setOutputStreamRedirection( System.out);
 *
 * 4.
 * RemoteAsyncCommandResult<IRemoteEcho> cmdRes = Executor.executeAsync( rjavaCmd, null);
 *
 * 5.
 * IRemoteEcho echoStub = cmdRes.getRemoteStub();
 *
 * 6.
 * String echoMsg = echoStub.sayEcho("TGrid");
 * System.out.println(echoMsg); // output Hello: TGrid
 *
 * 7.
 * cmdRes.stop();
 * </pre>
 *
 * {@link RemoteAsyncCommandResult#getProcessAdmin()} returns admin object of forked JVM process.
 * <br> <u>NOTE:</u> RemoteProcessClass receives in constructor must have public default constructor
 * without no arguments. <br> On attempt to invoke remote call on stopped/destroyed process,
 * java.rmi.ConnectException will be thrown.<br>
 *
 * DEBUG: To turn on tracing of remote method sent to forked JVM set logging:
 * <u>com.gigaspaces.tgrid.client.executor.level=FINER</u>
 *
 * @author Igor Goldenberg
 * @see JavaCommand
 * @see Executor#executeAsync(RemoteJavaCommand, java.io.File)
 * @since 1.0
 **/
public class RemoteJavaCommand<R extends Remote>
        extends JavaCommand {

    final static private Log _logger = LogFactory.getLog(RemoteJavaCommand.class);

    private R _javaProcessStub;
    @SuppressWarnings("rawtypes")
    final private Class _remoteClass;
    private IRemoteJavaProcessAdmin _remoteJavaProcess;

    /**
     * Dynamic proxy invocation handler intercept all local calls and performs RMI call to {@link
     * RemoteJavaProcess}. We must keep no dependences of GigaSpaces jars and don't use
     * GenericExporter, only JRMP.
     **/
    final static private class JavaCmdInvocationHandler
            implements InvocationHandler {
        final private String _remoteProcessClass;
        final private InternalRemoteJavaProcessProtocol _remoteInvHandler;

        private JavaCmdInvocationHandler(String remoteProcessClass, InternalRemoteJavaProcessProtocol remoteInvHandler) {
            _remoteProcessClass = remoteProcessClass;
            _remoteInvHandler = remoteInvHandler;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (_logger.isDebugEnabled()) {
                String argsStr = args != null ? ("args: " + Arrays.asList(args) + ", ") : "";
                _logger.debug("RemoteJavaCommand invokes method: [" + method + "], " + argsStr + "class: [" + _remoteProcessClass + "]");
            }

            try {
                /* invokes remotely, we may improve RMI calls by cache methods signature and sent only method ID */
                return _remoteInvHandler.invoke(method.getName(), method.getParameterTypes(), args);
            } catch (InvocationTargetException ex) {
                throw ex.getTargetException();
            } catch (ConnectException ex) {
                throw new ConnectException("Failed to invoke method: [" + method + "], remote process: [" + _remoteProcessClass + "] was destroyed.");
            }
        }
    }

    /**
     * Simple constructor to instantiate RemoteJavaCommand.
     *
     * @param remoteClass remote class implementation.
     * @param classPath   the classpath of JVM process or use {@link ClassPath#inherit()}
     * @param mainArgs    the main arguments if main() method exists or nothing to pass.
     * @throws IllegalArgumentException if remoteClass doesn't have public no args constructor.
     */
    public RemoteJavaCommand(Class<? extends R> remoteClass, ClassPath classPath, String... mainArgs) {
        this(remoteClass, classPath, null/*sysProp*/, null/*jvmArg*/, mainArgs);
    }

    /**
     * Advance constructor to instantiate RemoteJavaCommand with desired classpath, system
     * properties and jvm args.
     *
     * @param remoteClass remote class implementation.
     * @param classPath   the classpath of JVM process or use {@link ClassPath#inherit()}
     * @param systemProp  optional system properties or <code>null</code>
     * @param jvmArgs     optional jvm args or <code>null</code>
     * @param mainArgs    optinal main arguments if main method exists or <code>null</code>.
     * @throws IllegalArgumentException if remoteClass doesn't have public no args constructor.
     */
    public RemoteJavaCommand(Class<? extends R> remoteClass, ClassPath classPath, Map<String, String> systemProp, Argument[] jvmArgs, String[] mainArgs) {
        super(remoteClass.getName(), classPath, systemProp, jvmArgs, mainArgs);

        ExecutorUtils.ensureValidClass(remoteClass);

        _remoteClass = remoteClass;

        /* suppress rmi DGC to call full GC, every 1 hour */
        addSystemPropParameter("sun.rmi.dgc.client.gcInterval", "3600000");
        addSystemPropParameter("sun.rmi.dgc.server.gcInterval", "3600000s");
    }

    /**
     * @return remote stub to forked JVM
     */
    protected R getRemoteStub() {
        if (_javaProcessStub == null)
            throw new IllegalStateException("No reference to remote process stub. Either " +
                    "1. Failed to fork process, or 2. failed to lookup process");

        return _javaProcessStub;
    }

    /**
     * @return lookup in RMIRegistry reference stub to remote JVM by supplied unique command UID
     */
    protected InternalRemoteJavaProcessProtocol lookupRemoteJavaProcess(String commandUID)
            throws ForkProcessException {
        /** number of retries (equivalent to sec) to find forkable service in JNDI */
        final long SLEEP_BETWEEN_RETRIES = 1000; //1 second
        final long LOOKUP_TIMEOUT = 5 * 60 * SLEEP_BETWEEN_RETRIES; //5 min

        long lookupTimeout = LOOKUP_TIMEOUT;
        RegistryException firstCaughtException = null;

        while (lookupTimeout > 0) {
            try {
                InternalRemoteJavaProcessProtocol procController = (InternalRemoteJavaProcessProtocol) getProcessRegistry().lookup(commandUID);

                /* check that we don't get a zombi RemoteJavaProcess instance */
                procController.isAlive();
                return procController;
            } catch (RegistryException re) {
                if (firstCaughtException == null)
                    firstCaughtException = re;

                if (_logger.isDebugEnabled()) {
                    _logger.debug(
                            "Lookup of RemoteJavaProcess UUID: [" + commandUID
                                    + "] caught: " + re + "\n"
                                    + "Retrying every ["
                                    + SLEEP_BETWEEN_RETRIES + " ms], timeout in ["
                                    + lookupTimeout + " ms]", re);
                }

                //sleep and retry
                try {
                    Thread.sleep(SLEEP_BETWEEN_RETRIES);
                    lookupTimeout -= SLEEP_BETWEEN_RETRIES;
                } catch (InterruptedException ie) {
                    throw new ForkProcessException(
                            "Lookup of RemoteJavaProcess UUID: [" + commandUID + "] was interrupted.", ie);
                }
            } catch (ForkProcessException fpe) {
                throw fpe;
            } catch (Exception e) {
                //wrap any other exception
                throw new ForkProcessException("Failed to lookup RemoteJavaProcess UUID: [" + commandUID + "]", e);
            }
        }

        throw new ForkProcessException("Lookup of RemoteJavaProcess UUID: [" + commandUID + "] has timedout after [" + LOOKUP_TIMEOUT + "] ms.", firstCaughtException);
    }

    /**
     * callback from {@link Executor} after fork process
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void afterExecute(Process process)
            throws ExecutionException {
        if (!Executor.isProcessAlive(process)) {
            throw new ExecutionException("Failed to fork process");
        }

        try {
            InternalRemoteJavaProcessProtocol remoteJvmProc = lookupRemoteJavaProcess(getUID());
            setRemoteJavaProcess(remoteJvmProc);

            /* create dynamic proxy of remote java process to be castable to all remote interfaces of supplied _remoteClass */
            Class[] remoteInterfaces = ExecutorUtils.getRemoteDeclaredInterfaces(_remoteClass);
            _javaProcessStub = (R) Proxy.newProxyInstance(_remoteClass.getClassLoader(), remoteInterfaces, new JavaCmdInvocationHandler(_remoteClass.getName(), remoteJvmProc));

        } catch (ForkProcessException e) {
            throw new ExecutionException("Failed to locate forkable java process.", e);
        }
    }

    /**
     * @return a main class of this RemoteJavaCommand
     */
    @Override
    protected Class<? extends JavaProcess> getJavaProcessClass() {
        return RemoteJavaProcess.class;
    }

    /**
     * set remote java process of forked JVM
     */
    protected void setRemoteJavaProcess(IRemoteJavaProcessAdmin remoteJavaProcessAdmin) {
        _remoteJavaProcess = remoteJavaProcessAdmin;
    }

    /**
     * @return an admin object of forked JVM
     */
    protected IRemoteJavaProcessAdmin getProcessAdmin() {
        if (_remoteJavaProcess == null)
            throw new IllegalArgumentException("This command is not executed. Operation will available only after Executer.execute()");

        return _remoteJavaProcess;
    }
}