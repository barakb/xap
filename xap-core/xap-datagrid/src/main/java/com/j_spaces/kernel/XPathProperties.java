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

package com.j_spaces.kernel;

import com.j_spaces.core.Constants;

/**
 * Class holds the commonly used XPath constants for the container and space overrides. The common
 * usage of these constants would be with custom properties object which is injected to the system
 * (through SpaceFinder, cli etc) and contains key/value pairs. If the key is an xpath of one of the
 * space or container config xml element, then the value will be injected into the system (into
 * com.j_spaces.kernel.log.JProperties) and will override ANY other value which was set before, even
 * system properties.
 *
 * @author Gershon Diner
 * @version 1.0
 * @since 5.1
 */
public interface XPathProperties {
    /**
     * xpath used to set the rmi registry enable flag as well as the url (host:port)
     */
    final String CONTAINER_JNDI_ENABLED = "com.j_spaces.core.container.directory_services.jndi.enabled";
    final String CONTAINER_JNDI_URL = "com.j_spaces.core.container.directory_services.jndi.url";

    /**
     * xpath used to set the Jini LUS list of unicast hosts separated by a comma
     */
    final String CONTAINER_JINI_LUS_UNICAST_HOSTS = "com.j_spaces.core.container.directory_services.jini_lus.unicast_discovery.lus_host";

    /**
     * xpath used to set the Jini LUS unicast enabled flag
     */
    final String CONTAINER_JINI_LUS_UNICAST_ENABLED = "com.j_spaces.core.container.directory_services.jini_lus.unicast_discovery.enabled";

    /**
     * xpath used to set the Jini LUS list of the groups separated by a comma
     */
    final String CONTAINER_JINI_LUS_GROUPS = Constants.LookupManager.LOOKUP_GROUP_PROP;


}
