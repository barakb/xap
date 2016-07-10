/*******************************************************************************
 * Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
package org.jini.rio.boot;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public abstract class CustomURLClassLoader extends URLClassLoader {

    private static final char SEPARATOR = File.pathSeparatorChar;
    private static final String NEW_LINE = System.getProperty("line.separator");

    private final String name;

    protected CustomURLClassLoader(String name, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.name = name;
    }

    /**
     * Returns a String representation of this class loader.
     **/
    public String toString() {
        return (super.toString() + " Name : [" + name + "]");
    }

    public String getName() {
        return this.name;
    }

    public void dump(StringBuilder sb) {
        sb.append(super.toString());
        dumpDetails(sb);
    }

    protected void dumpDetails(StringBuilder sb) {
        sb.append(" name=").append(getName());
        sb.append(" URLs:" + NEW_LINE);
        for (URL url : getURLs()) {
            sb.append(url + NEW_LINE);
        }
        sb.append("*** End of URLs ***" + NEW_LINE);
    }

    public static void append(StringBuilder sb, URL url) {
        final File file = new File(url.getFile());
        sb.append(file.getAbsolutePath());
        if (!file.exists())
            sb.append("(not exists)");
    }

    public static void append(StringBuilder sb, URL[] urls) {
        for (final URL url : urls) {
            append(sb, url);
            sb.append(SEPARATOR);
        }
    }

    public static void append(StringBuilder sb, List<URL> urls) {
        for (final URL url : urls) {
            append(sb, url);
            sb.append(SEPARATOR);
        }
    }
}
