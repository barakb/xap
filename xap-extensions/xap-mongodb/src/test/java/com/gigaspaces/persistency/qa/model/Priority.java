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

package com.gigaspaces.persistency.qa.model;

/**
 * Priority of this Issue; default trivial. Priority increases at vote-levels.
 */
public enum Priority {

    TRIVIAL, MINOR, MEDIUM, MAJOR, CRITICAL, BLOCKER;

    public static boolean isTrivial(Priority p) {
        return p == TRIVIAL;
    }

    public static boolean isMinor(Priority p) {
        return p == MINOR;
    }

    public static boolean isMedium(Priority p) {
        return p == MEDIUM;
    }

    public static boolean isMajor(Priority p) {
        return p == MAJOR;
    }

    public static boolean isCritical(Priority p) {
        return p == CRITICAL;
    }

    public static boolean isBlocker(Priority p) {
        return p == BLOCKER;
    }

}
