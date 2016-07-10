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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.properties.BeanLevelProperties;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * @author kimchy
 */
public abstract class BeanLevelPropertiesUtils {

    private static final Log logger = LogFactory.getLog(BeanLevelPropertiesUtils.class);

    /**
     * Prefix for system property placeholders: "${"
     */
    public static final String PLACEHOLDER_PREFIX = "${";

    /**
     * Suffix for system property placeholders: "}"
     */
    public static final String PLACEHOLDER_SUFFIX = "}";

    public static void resolvePlaceholders(BeanLevelProperties beanLevelProperties, File input) throws IOException {
        if (!input.exists()) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Replacing file [" + input.getAbsolutePath() + "] with properties [" + beanLevelProperties.getContextProperties() + "]");
        }
        String content = FileCopyUtils.copyToString(new InputStreamReader(new FileInputStream(input), "UTF-8"));
        File renameTo = new File(input.getAbsolutePath() + ".replace");
        boolean success = false;
        for (int i = 0; i < 10; i++) {
            if (input.renameTo(renameTo)) {
                success = true;
                break;
            }
        }
        if (!success) {
            throw new IOException("Failed to rename [" + input.getAbsolutePath() + "] in order to replace it");
        }
        FileCopyUtils.copy(resolvePlaceholders(content, beanLevelProperties), new OutputStreamWriter(new FileOutputStream(input)));
        renameTo.delete();
    }

    public static String resolvePlaceholders(String text, BeanLevelProperties beanLevelProperties) {
        StringBuilder buf = new StringBuilder(text);

        int startIndex = buf.indexOf(PLACEHOLDER_PREFIX);
        while (startIndex != -1) {
            int endIndex = buf.indexOf(PLACEHOLDER_SUFFIX, startIndex + PLACEHOLDER_PREFIX.length());
            if (endIndex != -1) {
                String placeholder = buf.substring(startIndex + PLACEHOLDER_PREFIX.length(), endIndex);
                int nextIndex = endIndex + PLACEHOLDER_SUFFIX.length();
                try {
                    String propVal = beanLevelProperties.getContextProperties().getProperty(placeholder);
                    if (propVal == null) {
                        propVal = System.getProperty(placeholder);
                        if (propVal == null) {
                            // Fall back to searching the system environment.
                            propVal = System.getenv(placeholder);
                        }
                    }
                    if (propVal != null) {
                        buf.replace(startIndex, endIndex + PLACEHOLDER_SUFFIX.length(), propVal);
                        nextIndex = startIndex + propVal.length();
                    }
                } catch (Throwable ex) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Could not resolve placeholder '" + placeholder + "' in [" + text +
                                "] as system property: " + ex);
                    }
                }
                startIndex = buf.indexOf(PLACEHOLDER_PREFIX, nextIndex);
            } else {
                startIndex = -1;
            }
        }

        return buf.toString();
    }
}
