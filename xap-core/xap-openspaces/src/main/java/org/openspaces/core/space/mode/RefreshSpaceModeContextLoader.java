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


package org.openspaces.core.space.mode;

import org.jini.rio.boot.ServiceClassLoader;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Extension to SpaceModeContextLoader allowing for the context loader to be refreshable and load
 * new code. Only works with the Service Grid or the Standalone container.
 *
 * <p>If a refresh is executed on this context loader, will close the application context of this
 * context and load it again using a new class loader allowing for new code definitions to be
 * applied.
 *
 * @author kimchy
 */
public class RefreshSpaceModeContextLoader extends SpaceModeContextLoader implements RefreshableContextLoader {

    private ClassLoader classLoader;

    private ClassLoader childAppContextClassLoader;

    @Override
    public void afterPropertiesSet() throws Exception {
        classLoader = Thread.currentThread().getContextClassLoader();
        super.afterPropertiesSet();
    }

    @Override
    protected void loadApplicationContext() throws Exception {
        if (applicationContext != null) {
            return;
        }
        if (classLoader.getClass().equals(URLClassLoader.class)) {
            URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            childAppContextClassLoader = new URLClassLoader(urlClassLoader.getURLs(), urlClassLoader.getParent());
        } else if (classLoader instanceof ServiceClassLoader) {
            ServiceClassLoader serviceClassLoader = (ServiceClassLoader) classLoader;
            String name = serviceClassLoader.getLogName();
            if (beanName != null) {
                name = name + "/" + beanName;
            } else {
                name = name + "/{refreshable}";
            }
            childAppContextClassLoader = new ServiceClassLoader(name, new URL[0], serviceClassLoader.getClassAnnotator(), serviceClassLoader); // we rely on parent last here
            ((ServiceClassLoader) childAppContextClassLoader).setLibPath(serviceClassLoader.getLibPath());
            ((ServiceClassLoader) childAppContextClassLoader).setSlashPath(serviceClassLoader.getSlashPath());
        } else {
            logger.warn("Can't handle class loader [" + classLoader + "], context refreshing requires the service grid class loader or the url class loader. Context refreshing is disabled.");
            childAppContextClassLoader = classLoader;
        }
        ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(childAppContextClassLoader);
        try {
            super.loadApplicationContext();
        } finally {
            Thread.currentThread().setContextClassLoader(origClassLoader);
        }
    }

    @Override
    protected void closeApplicationContext() {
        ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(childAppContextClassLoader);
        try {
            super.closeApplicationContext();
        } finally {
            Thread.currentThread().setContextClassLoader(origClassLoader);
        }
    }

    @Override
    public void refresh() throws Exception {
        closeApplicationContext();
        loadApplicationContext();
    }
}
