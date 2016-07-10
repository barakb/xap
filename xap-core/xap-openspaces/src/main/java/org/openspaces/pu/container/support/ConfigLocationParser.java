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


package org.openspaces.pu.container.support;

import org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider;

import java.io.IOException;

/**
 * Parses multiple -config parameter by adding it to {@link org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider#addConfigLocation(String)}.
 *
 * @author kimchy
 */
public abstract class ConfigLocationParser {

    public static final String CONFIG_PARAMETER = "config";

    /**
     * Parses multiple -config parameter by adding it to {@link org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider#addConfigLocation(String)}.
     *
     * @param containerProvider The container provider to add the config location to
     * @param params            The parameters to parse for -config options
     */
    public static void parse(ApplicationContextProcessingUnitContainerProvider containerProvider,
                             CommandLineParser.Parameter[] params) throws IOException {
        // parse the config location parameters
        for (int i = 0; i < params.length; i++) {
            if (params[i].getName().equalsIgnoreCase(CONFIG_PARAMETER)) {
                for (int j = 0; j < params[i].getArguments().length; j++) {
                    containerProvider.addConfigLocation(params[i].getArguments()[j]);
                }
            }
        }
    }
}
