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

/**
 *
 */
package com.j_spaces.kernel;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Helper class that converts time interval property to milliseconds long value, according to the
 * given time unit format ms - milliseconds s - seconds m - minutes h - hours
 *
 * @author anna
 * @since 6.1
 */
public enum TimeUnitProperty {
    MILLISECONDS("ms", 1), SECONDS("s", 1000), MINUTES("m", 60 * 1000), HOURS(
            "h", 60 * 60 * 1000);

    static final String TIME_PATTERN = "(\\d+)";

    private String regex;
    int multiplier;

    /**
     *
     */
    TimeUnitProperty(String suffix, int multiplier) {
        this.regex = TIME_PATTERN + suffix;
        this.multiplier = multiplier;
    }

    /**
     * Parse given input according to the regular expression and extract the time value
     */
    Long parse(String time) {
        Pattern pattern = Pattern.compile(regex);

        Matcher m = pattern.matcher(time);

        if (m.matches()) {
            String timeValue = m.group(1);
            return Long.parseLong(timeValue) * multiplier;
        }
        return null;
    }

    /**
     * Parse given input according to the default time unit
     */
    Long parseDefault(String time) {
        Pattern pattern = Pattern.compile(TIME_PATTERN);

        Matcher m = pattern.matcher(time);

        if (m.matches()) {
            String timeValue = m.group(1);
            return Long.parseLong(timeValue) * multiplier;
        }
        return null;
    }

    /**
     * Get the property from the system properties and parse it according the matching pattern.
     * Default time unit is seconds
     *
     * @return system property according the matching pattern
     */
    public static long getProperty(String key, String def) {
        return getProperty(key, def, TimeUnitProperty.SECONDS);
    }

    /**
     * Get the property from the system properties and parse it according the matching pattern
     *
     * @return timeValue
     */
    public static long getProperty(String key, String def,
                                   TimeUnitProperty defaultTimeUnit) {
        String time = System.getProperty(key, def);

        Long timeValue = null;

        for (TimeUnitProperty unit : TimeUnitProperty.values()) {
            timeValue = unit.parse(time);
            if (timeValue != null)
                return timeValue;
        }

        // default pattern is in seconds
        timeValue = defaultTimeUnit.parseDefault(time);

        if (timeValue != null)
            return timeValue;

        throw new IllegalArgumentException("Invalid time interval pattern.");
    }

    /**
     * Parse the time using the provided TimeUnitProperty. This method does not get the value from
     * system properties but instead uses the passed value and returns a long representation.
     */
    public static long getParsedValue(String time,
                                      TimeUnitProperty defaultTimeUnit) {
        Long timeValue = null;

        for (TimeUnitProperty unit : TimeUnitProperty.values()) {
            timeValue = unit.parse(time);
            if (timeValue != null)
                return timeValue;
        }

        // default pattern is in seconds
        timeValue = defaultTimeUnit.parseDefault(time);

        if (timeValue != null)
            return timeValue;

        throw new IllegalArgumentException("Invalid time interval pattern.");
    }

    public static long getParsedValue(String time) {
        return getParsedValue(time, TimeUnitProperty.SECONDS);
    }

}
