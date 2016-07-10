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
 * @(#)JavaCommand.java   Apr 25, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This command is Java command that will execute a new Java VM to run some Java code. <br> Example
 * usage of <b>async-execution</b>:
 * <pre>
 *  ClassPath cp = new ClassPath("/usr/lib/mylib1.jar:/usr/lib/mylib2.jar:/usr/lib/mylib3.jar");
 *  cp.addEntry("/usr/lib/mynewlib.jar");
 *  cp.removeEntry("/usr/lib/mylib2.jar");
 *  cp.inherit();
 *
 *  JavaCommand javaCmd = new JavaCommand( TestMainClass.class.getName(), cp );
 * 	 javaCmd.addJVMArg( "-Xmx64m" );
 * 	 javaCmd.addSystemPropParameter("my.system.prop", "myvalue");
 * 	 javaCmd.addMainArgs( "arg1", "arg2", "arg3" );
 * 	 javaCmd.setOutputStreamRedirection( System.out);
 *
 * 	 AsyncCommandResult asc = Executor.executeAsync( javaCmd , null);
 *
 *  <b>or sync execution:</b>
 *
 *  // sync execution blocks while process is running, Executor returns CommandResult when process
 * finished to run.
 * 	CommandResult cmdRes = Executor.execute( javaCmd , null);
 *  int exitCode = cmdRes.getCode();  // process exit code
 *
 * </pre>
 * NOTE: If this(parent) JVM was destroyed, the forked JVM will be destroyed as well by forked
 * WatchDog process.<br> The Watchdog of forked JVM periodically every 15 sec checks liveness of
 * parent JVM.
 *
 * @author Igor Goldenberg
 * @see RemoteJavaCommand
 * @since 1.0
 **/
public class JavaCommand
        implements Command {
    private String _javaBin = System.getProperty("java.home") + "/bin/java";
    private final String _mainClassName;
    private ClassPath _classPath;
    private final Map<String, String> _systemProp;
    private final List<Argument> _jvmArgs;
    private final List<String> _mainArgs;
    private OutputStream _outputStreamRedirection = System.out;


    public final static String PARENT_PROCESS_UID = "puid";
    public final static String PROCESS_UID = "uid";
    public final static String MAIN_CLASS = "mainclass";

    private static final ProcessRegistry processRegistry = ProcessRegistry.createRegistry();

    /**
     * Creates a new JavaCommand that will execute a new Java process with desired classpath and
     * main arguments.
     *
     * @param mainClassName the main classname contains a main() method.
     * @param classpath     the classpath for this JavaCommand or <code>null</code> to inherit the
     *                      classpath from this(parent) JVM.
     * @param main          method argumethods.
     **/
    public JavaCommand(String mainClassName, ClassPath classPath, String... mainArgs) {
        this(mainClassName, classPath, null/*sysProp*/, null/*jvmArgs*/, mainArgs);
    }

    /**
     * Creates a new JavaCommand that will execute a new Java process with desired java executable,
     * classpath, system properties and jvmArgs.
     *
     * @param javaBin       path to java binary, this allow to launch JavaCommand with desired JVM
     *                      type(Sun/JRockit,IBM...).
     * @param mainClassName the main classname contains a main() method.
     * @param classpath     the classpath for this JavaCommand or <code>null</code> to inherit the
     *                      classpath from this(parent) JVM.
     * @param systemProp    the system properties with key and value for i.e:
     *                      java.util.logging.config.file=/usr/log/log.properties. The key may also
     *                      be with -D.
     * @param jvmArgs       the JVM args. For i.e: -Xmx128m
     * @param main          method argumethods.
     **/
    public JavaCommand(String javaBin, String className, ClassPath classPath, Map<String, String> systemProp, Argument[] jvmArgs, String... mainArgs) {
        this(className, classPath, systemProp, jvmArgs, mainArgs);

        if (javaBin != null)
            _javaBin = javaBin;
    }

    /**
     * Creates a new JavaCommand that will execute a new Java process with desired classpath, system
     * properties and jvmArgs.
     *
     * @param mainClassName the main classname contains a main() method.
     * @param classpath     the classpath for this JavaCommand or <code>null</code> to inherit the
     *                      classpath from this(parent) JVM.
     * @param systemProp    the system properties with key and value for i.e:
     *                      java.util.logging.config.file=/usr/log/log.properties. The key may also
     *                      be with -D.
     * @param jvmArgs       the JVM args. For i.e: -Xmx128m
     * @param main          method argumethods.
     **/
    public JavaCommand(String mainClassName, ClassPath classPath, Map<String, String> systemProp, Argument[] jvmArgs, String... mainArgs) {
        _jvmArgs = new ArrayList<Argument>();
        _systemProp = new LinkedHashMap<String, String>();
        _mainArgs = new ArrayList<String>();

        _mainClassName = mainClassName;
        if (classPath == null) {
            _classPath = new ClassPath();
            _classPath.inherit();
        } else
            _classPath = classPath;

        // add tgrid.jar library
        _classPath.addEntry(ExecutorUtils.getClassLocation(getClass()));


        _mainArgs.addAll(ExecutorUtils.asList(mainArgs));

        /* inherit the defined parent uid from parent process, and generate own uid */
        String parentProcessUID = System.getProperty(JavaCommand.PARENT_PROCESS_UID, UUID.randomUUID().toString() + "@" + processRegistry.getRegistryPort());
        addSystemPropParameter(JavaCommand.PARENT_PROCESS_UID, parentProcessUID);

        String procUID = UUID.randomUUID().toString() + "@" + processRegistry.getRegistryPort();
        addSystemPropParameter(JavaCommand.PROCESS_UID, procUID);
        addSystemPropParameter(MAIN_CLASS, mainClassName);

        if (systemProp != null)
            _systemProp.putAll(systemProp);

        if (jvmArgs != null)
            _jvmArgs.addAll(ExecutorUtils.asList(jvmArgs));
    }

    /**
     * @return the main class of this JavaCommand
     */
    public String getMainClass() {
        return _mainClassName;
    }

    /**
     * @return main class arguments String[]
     */
    public String[] getMainArgs() {
        return _mainArgs.toArray(new String[0]);
    }

    /**
     * @return the arguments that are passed to the command.
     */
    public String[] getCommand() {
        return Argument.toString(getArguments());
    }

    /**
     * Sets the output stream to redirect this commands output. Convenient method wrapping the use
     * of {@link FileOutputStream}.
     *
     * @param fileName The file name to redirect output to.
     * @param append   true if output should be appended to the file, false otherwise.
     * @throws FileNotFoundException if the file exists but is a directory rather than a regular
     *                               file, does not exist but cannot be created, or cannot be opened
     *                               for any other reason.
     * @throws SecurityException     if a security manager exists and its checkWrite method denies
     *                               write access to the file.
     */
    public void setOutputStreamRedirection(String fileName, boolean append)
            throws FileNotFoundException {
        setOutputStreamRedirection(new FileOutputStream(fileName, append));
    }

    /**
     * Sets the output stream to redirect this commands output.
     *
     * @param outputStream an output stream.
     */
    public void setOutputStreamRedirection(OutputStream outputStream) {
        _outputStreamRedirection = outputStream;
    }

    /**
     * The output stream to redirect the output to. If none was set, will redirect to System.out.
     * Set to <code>null</code> if redirection is not required.
     *
     * @return The output stream set by {@link #setOutputStreamRedirection(OutputStream)}, or
     * <code>System.out</code> if none was set, or <code>null</code> if redirection is not
     * required.
     */
    public OutputStream getOutputStreamRedirection() {
        return _outputStreamRedirection;
    }

    /**
     * @return jvm arguments.
     */
    public void addJVMArg(Argument arg) {
        if (arg == null)
            throw new IllegalArgumentException("Argument is null");

        _jvmArgs.add(arg);
    }

    /**
     * Add JVM argument.
     *
     * @param args jvm argument.
     */
    public void addJVMArg(String args) {
        if (args == null)
            return;

        List<String> listJvmArgs = ExecutorUtils.tokenize(args, null /* default */);
        for (String sp : listJvmArgs)
            addJVMArg(new Argument(sp));
    }

    /**
     * Add system properties as one argument line, for i.e -Dcom.gs.arg1=test1 -Dcom.gs.arg2=test2
     * ...
     */
    public void addSystemProperties(String sysProp) {
        if (sysProp == null)
            return;

        List<String> listSysProp = ExecutorUtils.tokenize(sysProp, null /* default */);
        for (String sp : listSysProp) {
            String[] pv = sp.split("=");
            _systemProp.put(pv[0], pv[1]);
        }
    }

    /**
     * Add system property name and value.
     *
     * @param key   system property name, without -D prefix.
     * @param value system property value.
     **/
    public void addSystemPropParameter(String key, String value) {
        if (key == null || value == null)
            throw new IllegalArgumentException(
                    "Illegal system property argument for key: ["
                            + key + "] value: [" + value + "]");

        _systemProp.put(key, value);
    }

    /**
     * Add main arguments.
     *
     * @param args main args.
     */
    public void addMainArgs(String... args) {
        for (String a : args)
            _mainArgs.add(a);
    }

    /**
     * {@inheritDoc}
     */
    public Argument[] getArguments() {
        ArrayList<Argument> cmdArgs = new ArrayList<Argument>();

        cmdArgs.add(new Argument(_javaBin));

        if (_jvmArgs.size() > 0) {
            for (Argument arg : _jvmArgs) {
                if (arg.getValue() != null)
                    cmdArgs.add(new Argument(arg.getName(), arg.getValue()));
                else
                    cmdArgs.add(new Argument(arg.getName()));
            }
        }

        if (_systemProp.size() > 0) {
            Iterator<Map.Entry<String, String>> iter = _systemProp.entrySet().iterator();
            while (iter.hasNext()) {
                StringBuilder sysPropBuilder = new StringBuilder();

                Map.Entry<String, String> iterEntry = iter.next();
                if (!iterEntry.getKey().startsWith("-D"))
                    sysPropBuilder.append("-D");

                sysPropBuilder.append(iterEntry.getKey());
                sysPropBuilder.append("=");
                sysPropBuilder.append(iterEntry.getValue());

                cmdArgs.add(new Argument(sysPropBuilder.toString()));
            }
        }

        cmdArgs.add(new Argument("-classpath", _classPath.toString()));

        if ("true".equalsIgnoreCase(System.getProperty("fork.useRetrotranslator", "false"))) {
            cmdArgs.add(new Argument("net.sf.retrotranslator.transformer.JITRetrotranslator"));
            cmdArgs.add(new Argument("-support", "ThreadLocal.remove"));
            cmdArgs.add(new Argument("-backport", "java.util.concurrent.locks.ReentrantReadWriteLock:edu.emory.mathcs.backport.java.util.concurrent.locks.ReentrantReadWriteLockWithFair"));
        }

        cmdArgs.add(new Argument(getJavaProcessClass().getName()));

        for (String arg : _mainArgs)
            cmdArgs.add(new Argument(arg));

        return cmdArgs.toArray(new Argument[cmdArgs.size()]);
    }

    /**
     * @return the main java process class
     */
    protected Class<? extends JavaProcess> getJavaProcessClass() {
        return JavaProcess.class;
    }

    /**
     * @return the unique ID of parent JavaCommand instance
     */
    public String getParentUID() {
        return _systemProp.get(PARENT_PROCESS_UID);
    }

    /**
     * @return the unique ID of this JavaCommand instance
     */
    public String getUID() {
        return _systemProp.get(PROCESS_UID);
    }

    /**
     * returns the singleton process registry
     */
    protected ProcessRegistry getProcessRegistry() {
        return processRegistry;
    }

    /**
     * @see org.openspaces.test.client.executor.Command#beforeExecute()
     */
    public void beforeExecute() {
    }

    /**
     * @see org.openspaces.test.client.executor.Command#afterExecute(Process)
     */
    public void afterExecute(Process process) {
    }

    /**
     * @param force if <code>true</code> the JavaCommand process will be forcibly destroyed by "kill
     *              -9" (relevant for Unix based machine)
     * @see org.openspaces.test.client.executor.Command#destroy(boolean)
     */
    public void destroy(boolean force) {
        try {
            processRegistry.unregister(getUID());
        } catch (RegistryException re) {
            ProcessLogger.log("Failed to unregister process", re);
        }

        forceKill(force ? getParentUID() : getUID());
    }

    /**
     * Kill JavaCommand by kill-9 with JavaCommand UID NOTE: This method only relevant for Unix
     * based machines.
     *
     * @param javaCmd the JavaCommand
     **/
    static void forceKill(String commandUID) {
        /* no kill command on Windows */
        if (!ExecutorUtils.isUnixOS())
            return;

        AsyncCommandResult killRes = null;
        try {
            // TODO update to pkill.sh location
            File binDir = new File("/bin");
            if (!binDir.exists())
                return;

            final String pkillScript = binDir.getPath() + File.separator + ExecutorContstants.KILL_TEST_SCRIPT;
            SimpleCommand killCmd = new SimpleCommand("Kill JVM", pkillScript, commandUID);
            killRes = Executor.executeAsync(killCmd, null);
            killRes.waitFor(5 * 1000);
        } catch (InterruptedException e) {
        } finally {
            /* destroy anyway */
            if (killRes != null)
                killRes.stop(false);
        }
    }

    @Override
    public String toString() {
        return Arrays.asList(getArguments()).toString();
    }
}