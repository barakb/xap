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

package com.gigaspaces.internal.lookup;

import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.client.SpaceURL;

import net.jini.core.discovery.LookupLocator;

/**
 * @author Niv Ingberg
 * @since 9.0.1
 */
public abstract class SpaceUrlUtils {
    public static boolean isRemoteProtocol(String spaceUrl) {
        return StringUtils.startsWithIgnoreCase(spaceUrl, SpaceURL.JINI_PROTOCOL) || StringUtils.startsWithIgnoreCase(spaceUrl, SpaceURL.RMI_PROTOCOL);
    }

    public static String toCustomUrlProperty(String propertyName) {
        return SpaceURL.PROPERTIES_SPACE_URL_ARG + "." + propertyName;
    }

    public static String buildJiniUrl(String containerName, String spaceName, String[] groups, LookupLocator[] locators) {
        StringBuilder sb = buildPrefix(containerName, spaceName);
        boolean hasProperties = false;
        hasProperties = appendGroups(sb, groups, hasProperties);
        hasProperties = appendLocators(locators, sb, hasProperties);

        // TODO: Check if security should be constructed as well.
        return sb.toString();
    }

    public static String buildJiniUrl(String containerName, String spaceName, String groups, String locators) {
        StringBuilder sb = buildPrefix(containerName, spaceName);
        boolean hasProperties = false;
        hasProperties = appendProperty(sb, SpaceURL.GROUPS, groups, hasProperties);
        hasProperties = appendProperty(sb, SpaceURL.LOCATORS, locators, hasProperties);

        // TODO: Check if security should be constructed as well.
        return sb.toString();
    }

    public static String setPropertyInUrl(String url, String name, String value) {
        return setPropertyInUrl(url, name, value, true);
    }

    public static String setPropertyInUrl(String url, String name, String value, boolean overrideIfExists) {
        final int startIndex = indexOf(url, name);
        if (startIndex == -1) {
            char separator = url.lastIndexOf('?') == -1 ? '?' : '&';
            url += separator + name + (value != null ? '=' + value : "");
        } else if (overrideIfExists) {
            // Find next parameter (if any), which signals end of current parameter:
            int endIndex = url.indexOf('&', startIndex + 1);
            if (endIndex < 0)
                endIndex = url.length();

            String newValue = StringUtils.hasLength(value) ? name + '=' + value : name;

            // replace existing parameter with new one
            StringBuilder sb = new StringBuilder(url);
            sb.replace(startIndex + 1, endIndex, newValue);
            url = sb.toString();
        }
        return url;
    }

    public static String deletePropertyFromUrl(String url, String name) {
        int startIndex = indexOf(url, name) + 1;
        if (startIndex == 0)
            return url;

        // Find next parameter (if any), which signals end of current parameter:
        int endIndex = url.indexOf('&', startIndex) + 1;
        if (endIndex == 0) {
            endIndex = url.length();
            startIndex--;
        }

        // replace existing parameter with new one
        StringBuilder sb = new StringBuilder(url);
        sb.delete(startIndex, endIndex);
        url = sb.toString();
        return url;
    }

    private static int indexOf(String url, String attName) {
        int pos;

        // check name=value pattern:
        if ((pos = StringUtils.indexOfIgnoreCase(url, '&' + attName + '=')) != -1)
            return pos;
        if ((pos = StringUtils.indexOfIgnoreCase(url, '?' + attName + '=')) != -1)
            return pos;

        // check boolean attribute with implicit value pattern:
        // Check first attribute:
        if ((pos = StringUtils.indexOfIgnoreCase(url, '?' + attName + '&')) != -1)
            return pos;
        // Check not-first-not-last attribute:
        if ((pos = StringUtils.indexOfIgnoreCase(url, '&' + attName + '&')) != -1)
            return pos;
        // Check last attribute:
        if (StringUtils.endsWithIgnoreCase(url, '?' + attName) || StringUtils.endsWithIgnoreCase(url, '&' + attName))
            return url.length() - attName.length() - 1;

        return -1;
    }

    private static StringBuilder buildPrefix(String containerName, String spaceName) {
        StringBuilder sb = new StringBuilder()
                .append(SpaceURL.JINI_PROTOCOL)
                .append("//")
                .append(SpaceURL.ANY)
                .append('/')
                .append(StringUtils.hasLength(containerName) ? containerName : SpaceURL.ANY)
                .append('/')
                .append(spaceName);
        return sb;
    }

    private static boolean appendProperty(StringBuilder sb, String key, String value, boolean hasProperties) {
        if (value == null || value.length() == 0)
            return hasProperties;

        sb.append(hasProperties ? '&' : '?');
        sb.append(key);
        sb.append('=');
        sb.append(value);
        return true;
    }

    private static boolean appendGroups(StringBuilder sb, String[] groups, boolean hasProperties) {
        if (groups == null || groups.length == 0)
            return hasProperties;

        sb.append(hasProperties ? '&' : '?');
        sb.append(SpaceURL.GROUPS);
        sb.append('=');
        sb.append(groups[0]);
        for (int i = 1; i < groups.length; i++) {
            sb.append(',');
            sb.append(groups[i]);
        }
        return true;
    }

    private static boolean appendLocators(LookupLocator[] locators, StringBuilder sb, boolean hasProperties) {
        if (locators == null || locators.length == 0)
            return hasProperties;

        sb.append(hasProperties ? '&' : '?');
        sb.append(SpaceURL.LOCATORS);
        sb.append('=');
        sb.append(locators[0].getHost());
        sb.append(':');
        sb.append(locators[0].getPort());
        for (int i = 1; i < locators.length; i++) {
            sb.append(',');
            sb.append(locators[i].getHost());
            sb.append(':');
            sb.append(locators[i].getPort());
        }
        return true;
    }
}
