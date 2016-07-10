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
 * @(#)ClassPath.java   Apr 25, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * This class models a ClassPath that contains a list of files that are directories or JAR/ZIP files
 * in the path. All class methods provides ability to add/remove dynamically the desired
 * jar/class-directory/zip. To inherit all classpath from running JVM use {@link #inherit()}
 * method.
 *
 * Example usage:
 * <pre>
 *  ClassPath cp = new ClassPath("/usr/lib/mylib1.jar:/usr/lib/mylib2.jar:/usr/lib/mylib3.jar");
 *  cp.addEntry("/usr/lib/mynewlib.jar");
 *  cp.removeEntry("/usr/lib/mylib2.jar");
 *  cp.inherit();
 *
 *  JavaCommand javaCmd = new JavaCommand( TestMainClass.class.getName(), cp );
 * 	 javaCmd.addJVMArg( "-Xmx64m" );
 * 	 javaCmd.addSystemPropParameter("-Dmy.system.prop", "myvalue");
 * 	 javaCmd.addMainArgs( "arg1", "arg2", "arg3" );
 * 	 javaCmd.setOutputStreamRedirection( System.out);
 *
 * 	 AsyncCommandResult asc = Executor.executeAsync( javaCmd , null);
 * </pre>
 *
 * @author Igor Goldenberg
 * @see JavaCommand
 * @see RemoteJavaCommand
 * @since 1.0
 **/
public class ClassPath {
    private List<String> _classpathList;

    public ClassPath() {
        _classpathList = new ArrayList<String>();
    }

    /**
     * Constructor with full classpath argument.
     *
     * @param classpath the classpath with path separator. Separator can be ; or : For example:
     *                  /usr/lib/mylib1.jar:/usr/lib/mylib2.jar:/usr/lib/mylib3.jar
     */
    public ClassPath(String classpath) {
        this();

        addClassPath(classpath);
    }

    /**
     * Copies the given classpath's entries into this new classpath.
     */
    public ClassPath(ClassPath classPath) {
        _classpathList.addAll(classPath._classpathList);
    }

    /**
     * Add classpath with path separator. Separator can be ; or : For example:
     * /usr/lib/mylib1.jar:/usr/lib/mylib2.jar:/usr/lib/mylib3.jar
     *
     * @param classpath the classpath with path separator.
     */
    protected void addClassPath(String classpath) {
        if (classpath == null)
            return;

        String sep = classpath.indexOf(";") == -1 ? ":" : ";";
        String[] parseCp = classpath.split(sep);

        for (String cpEntry : parseCp) {
            addEntry(cpEntry);
        }
    }

    /**
     * Inherit all classpath from running VM by System.getProperty("java.class.path")
     */
    public void inherit() {
        addClassPath(System.getProperty("java.class.path"));
    }

    /**
     * Adds all the files to the classpath
     */
    public void addAllFiles(File[] files) {
        for (File f : files)
            addEntry(f.getPath());
    }

    /**
     * Adds the entry to the classpath.
     */
    public void addEntry(String entry) {
        _classpathList.add(entry);
    }

    /**
     * Adds the file to the classpath by getting the file's absolute path and appending this string
     * to the classpath.
     */
    public void addFile(File file) {
        addEntry(file.getPath());
    }

    /**
     * Removes the entry from the classpath.
     */
    public void removeEntry(String entry) {
        _classpathList.remove(entry);
    }

    /**
     * Removes the existing file entry from the classpath.
     */
    public void removeFile(File file) {
        removeEntry(file.getPath());
    }

    /**
     * Converts the classpath to a platform compatible classpath String using the path separator
     * character from the File class.
     */
    public String toString() {
        StringBuilder classPathBuilder = new StringBuilder("");

        for (String cpEntry : _classpathList) {
            classPathBuilder.append(cpEntry);
            classPathBuilder.append(File.pathSeparatorChar);
        }

        return classPathBuilder.toString();
    }
}