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
package com.gigaspaces.internal.version;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlatformVersionTests {
    @Test
    public void testOfficialVersion() {
        String regex = loadProperties().getProperty("regex.officialversion");
        assertMatch(regex, "GigaSpaces XAP 12.0.0 M1 (build 15100-10, revision unspecified)");
        assertMatch(regex, "GigaSpaces XAP 12.0.0 GA (build 15100, revision bc78e667b511a68e0291f774151ff2bcd1115bf0)");
        assertMatch(regex, "GigaSpaces XAP 12.0.1 GA (build 15100, revision bc78e667b511a68e0291f774151ff2bcd1115bf0)");
        assertMatch(regex, "GigaSpaces XAP 12.1.0 GA (build 15100, revision bc78e667b511a68e0291f774151ff2bcd1115bf0)");
        assertMatch(regex, "GigaSpaces XAP 12.0.0 M6 (build 157980-62, revisions: xap=bc78e667b511a68e0291f774151ff2bcd1115bf0 xap-premium=31498ec3e6020a695ff7b93f6054d14f9f8789cb)");
        assertMatch(regex, PlatformVersion.getOfficialVersion());
    }

    private static void assertMatch(String regex, String s) {
        int count = patternCounter(regex, s);
        if (count != 1) {
            System.out.println("Regex: " + regex);
            System.out.println("Text:  " + s);
            Assert.fail("Pattern does not match string");
        }
    }

    private static int patternCounter(String regex, String source) {
        Pattern pattern = Pattern.compile(regex);
        Matcher patternCounterMatcher = pattern.matcher(source);
        ArrayList<String> patternsMatched = new ArrayList<String>();

        int counter = 0;
        while (patternCounterMatcher.find()) {
            patternsMatched.add(patternCounterMatcher.group());
            counter += 1;
        }

        return counter;
    }

    private Properties loadProperties() {
        final String path = "com/gigaspaces/internal/version/PlatformVersionTests.properties";
        Properties properties = new Properties();
        InputStream inputStream = PlatformVersionTests.class.getClassLoader().getResourceAsStream(path);
        try {
            properties.load(inputStream);
            inputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test resource", e);
        }

        for (String property : properties.stringPropertyNames()) {
            properties.setProperty(property, properties.getProperty(property).replace("\"", ""));
        }


        return properties;
    }
}
