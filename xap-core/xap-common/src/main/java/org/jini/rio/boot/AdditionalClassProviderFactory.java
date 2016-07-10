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
package org.jini.rio.boot;

/**
 * Factory containing a singleton {@link IAdditionalClassProvider}, currently used to load classed
 * from the LRMIClassLoadersHolder from the ServiceClassLoader due to backward reference between jar
 * dependencies
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class AdditionalClassProviderFactory {
    private static IAdditionalClassProvider _classProvider;

    //Force memory barrier
    public static synchronized void setClassProvider(IAdditionalClassProvider classProvider) {
        _classProvider = classProvider;
    }

    /**
     * loads a class
     *
     * @param className class name
     * @param fastPath  hint to the provider whether to use fast loading path only if supported
     * @return loaded class
     */
    public static Class<?> loadClass(String className, boolean fastPath) throws ClassNotFoundException {
        if (_classProvider == null) {
            //Force memory barrier
            synchronized (AdditionalClassProviderFactory.class) {
                if (_classProvider == null)
                    throw new ClassNotFoundException("no class provider present");
            }
        }

        return _classProvider.loadClass(className, fastPath);
    }
}
