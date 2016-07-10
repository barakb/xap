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

package org.openspaces.memcached.protocol;

import java.util.Arrays;

/**
 */
public enum Op {
    GET, GETS, APPEND, PREPEND, DELETE, DECR,
    INCR, REPLACE, ADD, SET, CAS, STATS, VERSION,
    QUIT, FLUSH_ALL, VERBOSITY;

    private static byte[][] ops = new byte[Op.values().length][];

    static {
        for (int x = 0; x < Op.values().length; x++)
            ops[x] = Op.values()[x].toString().toLowerCase().getBytes();
    }

    public static Op findOp(byte[] cmd) {
        for (int x = 0; x < ops.length; x++) {
            if (Arrays.equals(cmd, ops[x])) return Op.values()[x];
        }
        return null;
    }
}