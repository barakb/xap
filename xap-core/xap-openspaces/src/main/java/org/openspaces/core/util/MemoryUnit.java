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

package org.openspaces.core.util;


public enum MemoryUnit {
    BYTES(0), KILOBYTES(1), MEGABYTES(2), GIGABYTES(3), TERABYTES(4), PETABYTES(5), EXABYTES(6);

    /**
     * the index of this unit
     */
    private final int index;

    /**
     * Internal constructor
     */
    MemoryUnit(int index) {
        this.index = index;
    }

    /**
     * Lookup table for conversion factors
     */
    private static final long[] multipliers = {
            1L,
            1024L,
            1024L * 1024,
            1024L * 1024 * 1024,
            1024L * 1024 * 1024 * 1024,
            1024L * 1024 * 1024 * 1024 * 1024,
            1024L * 1024 * 1024 * 1024 * 1024 * 1024
    };

    /**
     * Lookup table to check saturation.  Note that because we are dividing these down, we don't
     * have to deal with asymmetry of MIN/MAX values.
     */
    private static final long[] overflows = {
            0, // unused
            Long.MAX_VALUE / 1024L,
            Long.MAX_VALUE / (1024L * 1024),
            Long.MAX_VALUE / (1024L * 1024 * 1024),
            Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024),
            Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024 * 1024),
            Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024 * 1024 * 1024),
    };

    /**
     * Lookup table for toPostfix()
     */
    private static final String[] postfixes = {
            "b",
            "k",
            "m",
            "g",
            "t",
            "p",
            "e",
    };

    /**
     * Perform conversion based on given delta representing the difference between units
     *
     * @param delta  the difference in index values of source and target units
     * @param memory the memory
     * @return converted memory or saturated value
     */
    private static long doConvert(int delta, long memory) {
        if (delta == 0)
            return memory;
        if (delta < 0)
            return memory / multipliers[-delta];
        if (memory > overflows[delta])
            return Long.MAX_VALUE;
        if (memory < -overflows[delta])
            return Long.MIN_VALUE;
        return memory * multipliers[delta];
    }

    /**
     * Convert the given memory capacity in the given unit to this unit.  Conversions from finer to
     * coarser granularities truncate, so lose precision. For example converting <tt>1023</tt> bytes
     * to kilobytes results in <tt>0</tt>. Conversions from coarser to finer granularities with
     * arguments that would numerically overflow saturate to <tt>Long.MIN_VALUE</tt> if negative or
     * <tt>Long.MAX_VALUE</tt> if positive.
     *
     * @param memory capacity in the given <tt>unit</tt>
     * @param unit   the unit of the <tt>memory</tt> argument
     * @return the converted memory in this unit, or <tt>Long.MIN_VALUE</tt> if conversion would
     * negatively overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     */
    public long convert(long memory, MemoryUnit unit) {
        return doConvert(unit.index - index, memory);
    }

    /**
     * Equivalent to <tt>BYTES.convert(memory, this)</tt>.
     *
     * @param memory the memory to convert to Bytes
     * @return the converted memory, or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     */
    public long toBytes(long memory) {
        return doConvert(index, memory);
    }

    public static long toBytes(String memory) {
        return valueOfToBytes(memory);
    }

    public long convert(String memoryCapacity) {
        return this.convert(MemoryUnit.valueOfToBytes(memoryCapacity), MemoryUnit.BYTES);
    }

    /**
     * Equivalent to <tt>KILOBYTES.convert(memory, this)</tt>.
     *
     * @param memory the memory to convert to Kilo Bytes
     * @return the converted memory, or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     */
    public long toKiloBytes(long memory) {
        return doConvert(index - KILOBYTES.index, memory);
    }

    public static long toKiloBytes(String memory) {
        return BYTES.toKiloBytes(toBytes(memory));
    }

    /**
     * Equivalent to <tt>MEGABYTES.convert(memory, this)</tt>.
     *
     * @param memory the memory to convert to Mega Bytes
     * @return the converted memory, or <tt>Long.MIN_VALUE</tt> if conversion would negatively
     * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
     * @see #convert
     */
    public long toMegaBytes(long memory) {
        return doConvert(index - MEGABYTES.index, memory);
    }

    public static long toMegaBytes(String memory) {
        return BYTES.toMegaBytes(toBytes(memory));
    }

    /**
     * Equivalent to <tt>GIGABYTES.convert(memory, this)</tt>.
     *
     * @param memory the memory to convert to Giga Bytes
     * @return the converted memory.
     * @see #convert
     */
    public long toGigaBytes(long memory) {
        return doConvert(index - GIGABYTES.index, memory);
    }

    public static long toGigaBytes(String memory) {
        return BYTES.toGigaBytes(toBytes(memory));
    }

    /**
     * Equivalent to <tt>TERABYTES.convert(memory, this)</tt>.
     *
     * @param memory the memory to convert to Tera Bytes
     * @return the converted memory.
     * @see #convert
     */
    public long toTeraBytes(long memory) {
        return doConvert(index - TERABYTES.index, memory);
    }

    public static long toTeraBytes(String memory) {
        return BYTES.toTeraBytes(toBytes(memory));
    }

    /**
     * Equivalent to <tt>PETABYTES.convert(memory, this)</tt>.
     *
     * @param memory the memory to convert to Peta Bytes
     * @return the converted memory.
     * @see #convert
     */
    public long toPetaBytes(long memory) {
        return doConvert(index - PETABYTES.index, memory);
    }

    public static long toPetaBytes(String memory) {
        return BYTES.toPetaBytes(toBytes(memory));
    }

    /**
     * Equivalent to <tt>EXABYTES.convert(memory, this)</tt>.
     *
     * @param memory the memory to convert to Exa Bytes
     * @return the converted memory.
     * @see #convert
     */
    public long toExaBytes(long memory) {
        return doConvert(index - EXABYTES.index, memory);
    }

    public static long toExaBytes(String memory) {
        return BYTES.toExaBytes(toBytes(memory));
    }

    public String toPostfix() {
        return postfixes[index];
    }

    private static long valueOfToBytes(String memoryCapacity) {
        int index = -1;
        if (memoryCapacity != null && memoryCapacity.length() > 1) {
            String memoryCapacityLowerCase = memoryCapacity.toLowerCase();
            String memoryUnitPostfix = memoryCapacityLowerCase.substring(memoryCapacityLowerCase.length() - 1);
            for (int i = 0; i < postfixes.length; i++) {
                if (postfixes[i].equals(memoryUnitPostfix)) {
                    index = i;
                    break;
                }
            }
        }

        if (index == -1) {
            index = 0; // default is bytes
        } else {
            //remove postfix
            memoryCapacity = memoryCapacity.substring(0, memoryCapacity.length() - 1);
        }

        return doConvert(index, Long.valueOf(memoryCapacity));
    }

    public String getPostfix() {
        return postfixes[index];
    }

}
