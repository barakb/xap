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

package com.j_spaces.core;

/**
 * A convenience class for constants.
 */
@com.gigaspaces.api.InternalApi
public class SpaceOperations {
    public final static int NOOP = 0;
    public final static int WRITE = 1;
    public final static int READ = 2;
    public final static int READ_IE = 3;
    public final static int TAKE = 4;
    public final static int TAKE_IE = 5;
    public final static int NOTIFY = 6;
    public final static int UPDATE = 7;
    public final static int LEASE_EXPIRATION = 8;
    public final static int RENEW_LEASE = 9;
}
