/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2006 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.jini.config;

/**
 */
@com.gigaspaces.api.InternalApi
public class AggregateConfig implements Configuration {
    Configuration common = null;

    private Configuration specific;

    /**
     * Create an AggregateConfig
     *
     * @param config     The "common" Configuration
     * @param configArgs Configuration arguments which may declare entries of the form
     *                   $component_entry. These entries will be resolved in the "common"
     *                   Configuration. Parsing of the end config would be done by ConfigurationFile
     *                   which delegates special entry resolution to this class.
     */
    public AggregateConfig(Configuration config, String[] configArgs) throws ConfigurationException {
        specific = ConfigurationProvider.getInstance(configArgs);
        common = config;
    }

    /**
     * Create an AggregateConfig
     *
     * @param config     - The "common" Configuration
     * @param configArgs - Configuration arguments which may declare entries of the form
     *                   $component_entry. These entries will be resolved in the "common"
     *                   Configuration. Parsing of the end config would be done by ConfigurationFile
     *                   which delegates special entry resolution to this class.
     * @param loader     - The class loader to use for interpreting class names
     */
    public AggregateConfig(Configuration config,
                           String[] configArgs,
                           ClassLoader loader) throws ConfigurationException {
        specific = ConfigurationProvider.getInstance(configArgs, loader);
        common = config;
    }

    /**
     * Create an AggregateConfig
     *
     * @param configs An array of configArgs, where the common config is specified as the last
     *                element of the array. The first element of the array defines the config which
     *                may may declare entries of the form $component_entry. These entries will be
     *                resolved in the "common" Configuration. Parsing of the end config would be
     *                done by ConfigurationFile which delegates special entry resolution to this
     *                class.
     */
    public AggregateConfig(String[] configs) throws ConfigurationException {
        specific = ConfigurationProvider.getInstance(configs);
    }

    /**
     * Create an AggregateConfig
     *
     * @param configs An array of configArgs, where the common config is specified as the last
     *                element of the array. The first element of the array defines the config which
     *                may may declare entries of the form $component_entry. These entries will be
     *                resolved in the "common" Configuration. Parsing of the end config would be
     *                done by ConfigurationFile which delegates special entry resolution to this
     *                class.
     * @param loader  - The class loader to use for interpreting class names
     */
    public AggregateConfig(String[] configs, ClassLoader loader)
            throws ConfigurationException {
        specific = ConfigurationProvider.getInstance(configs, loader);
    }

    /**
     * Get the common configuration this class was created with
     *
     * @return The common {@link net.jini.config.Configuration} object.
     */
    public Configuration getCommonConfiguration() {
        return (common);
    }

    /*
     * Helper to get the shared component name. Parameter name must be in the
     * format of $component_entry
     */
    private String getSharedComponent(String name) throws ConfigurationException {
        int ndx = name.lastIndexOf("_");
        if (ndx == -1)
            throw new ConfigurationException("unknown component name for " + name);
        String sharedComponent = name.substring(1, ndx);
        return (sharedComponent.replace('_', '.'));
    }

    /*
     * Helper to get the entry name. Parameter Name must be in the
     * format of $component_entry
     */
    private String getSharedEntryName(String name) throws ConfigurationException {
        int ndx = name.lastIndexOf("_");
        if (ndx == -1)
            throw new ConfigurationException("unknown component name for " + name);
        return (name.substring(ndx + 1));
    }


    public Object getEntry(String component, String name, Class type) throws ConfigurationException {
        try {
            return specific.getEntry(component, name, type);
        } catch (NoSuchEntryException e) {
            return common.getEntry(component, name, type);
        }
    }

    public Object getEntry(String component, String name, Class type, Object defaultValue) throws ConfigurationException {
        try {
            return specific.getEntry(component, name, type);
        } catch (NoSuchEntryException e) {
            return common.getEntry(component, name, type, defaultValue);
        }
    }

    public Object getEntry(String component, String name, Class type, Object defaultValue, Object data) throws ConfigurationException {
        Object retVal = specific.getEntry(component, name, type, null, data);
        if (retVal != null) {
            return retVal;
        }
        return common.getEntry(component, name, type, defaultValue, data);
    }


}