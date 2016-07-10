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

//Internal Doc
package com.gigaspaces.security.service;

import com.gigaspaces.internal.utils.PropertiesUtils;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.kernel.SystemProperties;

/**
 * Helper class for resolving if security is enabled.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class SecurityResolver {

    /**
     * system property resolver
     */
    private static final boolean SECURITY_ENABLED = Boolean.getBoolean(SystemProperties.SECURITY_ENABLED);

    /**
     * @return <code>true</code> if security has been enabled via system property;
     * <code>false</code> otherwise.
     */
    public static boolean isSecurityEnabled() {
        return SECURITY_ENABLED;
    }

    /**
     * @param spaceURL Space URL used to load the space.
     * @return <code>true</code> if security has been enabled via Space URL or system property;
     * <code>false</code> otherwise.
     */
    public static boolean isSecurityEnabled(SpaceURL spaceURL) {

        //security is disabled for local-cache and local-view; interception is done at secured remote space
        boolean useLocalCache = PropertiesUtils.getBoolean(spaceURL, SpaceURL.USE_LOCAL_CACHE, false);
        if (useLocalCache)
            return false;

        //has 'secured' in URL
        if (SecurityResolver.hasSecuredProperty(spaceURL))
            return true;

        //has -Dcom.gs.security.enabled=true and is not running inside GSC (we don't inherit security from GSC automatically)
        if (SECURITY_ENABLED && !Boolean.getBoolean("com.gigaspaces.gsc.running"))
            return true;

        return false;
    }

    /**
     * @param spaceURL space URL to extract the 'secured' property from
     * @return <code>true</code> if 'secured=true' property exists in the space URL or as a custom
     * property; <code>false</code> otherwise.
     */
    private static boolean hasSecuredProperty(SpaceURL spaceURL) {
        return PropertiesUtils.getBoolean(spaceURL, SpaceURL.SECURED, false)
                || PropertiesUtils.getBoolean(spaceURL.getCustomProperties(), SpaceURL.SECURED, false);
    }
}
