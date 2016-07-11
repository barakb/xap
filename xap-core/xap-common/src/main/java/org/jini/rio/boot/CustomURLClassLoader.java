/*******************************************************************************
 * Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
package org.jini.rio.boot;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public abstract class CustomURLClassLoader extends URLClassLoader implements LoggableClassLoader {

    private final String name;

    protected CustomURLClassLoader(String name, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.name = name;
    }

    @Override
    public String toString() {
        return (super.toString() + " [name=" + name + "]");
    }

    @Override
    public String getLogName() {
        return this.name;
    }
}
