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

package com.gigaspaces.persistency.qa.utils;

import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class AssertUtils {

    public static void assertEquivalent(String message, List expecteds,
                                        List actuals) {

        Collections.sort(expecteds);
        Collections.sort(actuals);

        for (int i = 0; i < actuals.size(); i++) {
            Assert.assertEquals(message, expecteds.get(i), actuals.get(i));
        }
    }

    public static void assertEquivalenceArrays(String message,
                                               Object[] expected, Object[] actual) {

        List<?> expected1 = Arrays.asList(expected);
        List<?> actual1 = Arrays.asList(actual);

        assertEquivalent(message, expected1, actual1);
    }
}
