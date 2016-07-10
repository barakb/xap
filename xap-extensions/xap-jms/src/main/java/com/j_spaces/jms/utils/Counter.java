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

import java.util.concurrent.locks.ReentrantLock;

/**
 * A generic thread-safe counter. Range between Integer.MIN_VALUE and Integer.MAX_VALUE.
 *
 * @author Shai Wolf
 */
public class Counter {
    /**
     * Synchronization lock.
     */
    protected ReentrantLock lock = new ReentrantLock();


    /**
     * The counter's value.
     */
    protected int value;


    /**
     * The counter's start value.
     */
    protected int start;


    /**
     * Creates a new Counter instance.
     */
    public Counter() {
        this(0);
    }


    /**
     * Creates a new Counter instance.
     *
     * @param startValue the start value of the counter, 0 by default.
     */
    public Counter(int startValue) {
        start = startValue;
        value = startValue;
    }


    /**
     * Resets the counter to the start value.
     */
    public void reset() {
        lock.lock();
        value = start;
        lock.unlock();
    }


    /**
     * Sets the value of the counter.
     *
     * @param val the new value.
     */
    public void setValue(int val) {
        lock.lock();
        value = val;
        lock.unlock();
    }


    /**
     * Returns the value of the counter.
     *
     * @return the value of the counter.
     */
    public int getValue() {
        lock.lock();
        int newValue = value;
        lock.unlock();
        return newValue;
    }


    /**
     * Increments the counter by 1.
     *
     * @return the new counter's value.
     */
    public int increment() {
        lock.lock();
        int newValue = ++value;
        lock.unlock();
        return newValue;
    }


    /**
     * Increments the counter by val.
     *
     * @param val the value by which to increment the counter.
     * @return the new counter's value.
     */
    public int increment(int val) {
        if (val < 0) {
            return decrement(-val);
        }
        lock.lock();
        int newValue = (value += val);
        lock.unlock();
        return newValue;
    }


    /**
     * Decrements the counter by 1.
     *
     * @return the new counter's value.
     */
    public int decrement() {
        lock.lock();
        int newValue = --value;
        lock.unlock();
        return newValue;
    }


    /**
     * Decrements the counter by val.
     *
     * @param val the value by which to decrement the counter.
     * @return the new counter's value.
     */
    public int decrement(int val) {
        if (val < 0) {
            return increment(-val);
        }
        lock.lock();
        int newValue = (value -= val);
        lock.unlock();
        return newValue;
    }


    /**
     * API method.
     */
    public String toString() {
        return String.valueOf(getValue());
    }
}