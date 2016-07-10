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

/*
 * @(#)UIDGen.java 1.0  27/04/2005 13:45:08
 */

package com.gigaspaces.lrmi;


/**
 * A <code>UIDGen</code> represents an identifier that is unique over time with respect to the host
 * it is generated on, or one of 2<sup>16</sup> "well-known" identifiers.
 *
 * <p>The {@link #nextId()} can be used to generate an identifier that is unique over time with
 * respect to the host it is generated on.
 *
 * <p>A <code>UIDGen</code> instance contains three primitive values: <ul> <li><code>unique</code>,
 * an <code>int</code> that uniquely identifies the VM that this <code>UID</code> was generated in,
 * with respect to its host and at the time represented by the <code>time</code> value (an example
 * implementation of the <code>unique</code> value would be a process identifier), or zero for a
 * well-known <code>UID</code> <li><code>time</code>, a <code>long</code> equal to a time (as
 * returned by {@link System#nanoTime()}) at which the VM that this <code>UID</code> was generated
 * in was alive, or zero for a well-known <code>UID</code> <li><code>count</code>, a
 * <code>short</code> to distinguish <code>UID</code>s generated in the same VM with the same
 * <code>time</code> value </ul>
 *
 * @author Igor Goldenberg
 * @since 4.0
 **/
@com.gigaspaces.api.InternalApi
public class UIDGen {
    private static long ONE_SECOND = 1000; // in milliseconds

    private static final Object lock = new Object();
    private static long lastTime = System.nanoTime();
    private static short lastCount = Short.MIN_VALUE;

    /**
     * a time (as returned by {@link SystemTime#timeMillis()}) at which the VM that this
     * <code>UID</code> was generated in was alive
     **/
    static private long time;

    /**
     * 16-bit number to distinguish <code>UID</code> instances created in the same VM with the same
     * time value
     **/
    static private short count;


    /**
     * Generates a <code>long</code> that is unique over time with respect to the host that it was
     * generated on.
     **/
    public static long nextId() {
        synchronized (lock) {
            if (lastCount == Short.MAX_VALUE) {
                boolean done = false;
                while (!done) {
                    long now = System.nanoTime();
                    if (now < lastTime + ONE_SECOND) {
                        // pause for a second to wait for time to change
                        try {
                            Thread.sleep(ONE_SECOND);
                        } catch (java.lang.InterruptedException e) {
                        } // ignore exception
                        continue;
                    } else {
                        lastTime = now;
                        lastCount = Short.MIN_VALUE;
                        done = true;
                    }
                }
            }

            time = lastTime;
            count = lastCount++;
        }

        return time + count;
    }
}