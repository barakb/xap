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

package com.j_spaces.jms.utils;

/**
 * This counter makes sure that we stay in the range of 0 - Integer.MAX_VALUE. The cyclic rule is:
 * ...Integer.MAX_VALUE-1, Integer.MAX_VALUE, 0, 1...
 *
 * @author Shai Wolf
 */
public class CyclicCounter extends Counter {
    /**
     * Creates a new CyclicCounter instance.
     */
    public CyclicCounter() {
        super();
    }


    /**
     * Creates a new CyclicCounter instance.
     *
     * @param startValue the start value of the counter, 0 by default.
     */
    public CyclicCounter(int startValue) {
        super(startValue);
    }


    /**
     * Increments the counter by 1.
     */
    public int increment() {
        lock.lock();
        if (value != Integer.MAX_VALUE) {
            ++value;
        } else {
            value = 0;
        }
        int newValue = value;
        lock.unlock();
        return newValue;
    }


    /**
     * Increments the counter by val.
     */
    public int increment(int val) {
        if (val < 0) {
            return decrement(-val);
        }
        lock.lock();
        int remain = Integer.MAX_VALUE - value;
        int dif = remain - val;
        if (dif >= 0) {
            value += val;
        } else {
            value = (-dif) - 1;
        }
        int newValue = value;
        lock.unlock();
        return newValue;
    }


    /**
     * Decrements the counter by 1.
     */
    public int decrement() {
        lock.lock();
        if (value != 0) {
            --value;
        } else {
            value = Integer.MAX_VALUE;
        }
        int newValue = value;
        lock.unlock();
        return newValue;
    }


    /**
     * Decrements the counter by val.
     */
    public int decrement(int val) {
        if (val < 0) {
            return increment(-val);
        }
        lock.lock();
        int dif = value - val;
        if (dif >= 0) {
            value -= val;
        } else {
            value = Integer.MAX_VALUE + (dif + 1);
        }
        int newValue = value;
        lock.unlock();
        return newValue;
    }
}