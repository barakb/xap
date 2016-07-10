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

package com.gigaspaces.security.authorities;

/**
 * Authority string format constants.
 *
 * @author Moran Avigdor
 */
public final class Constants {

    /**
     * Delimiter between template parts
     */
    public static final String DELIM = " ";

    /* Parts of the Authority string representation */
    public static final int PRIVILEGE_NAME_POS = 0;
    public static final int PRIVILEGE_VAL_POS = 1;
    public static final int FILTER_POS = 2;
    public static final int FILTER_PARAMS_POS = 3;
}
