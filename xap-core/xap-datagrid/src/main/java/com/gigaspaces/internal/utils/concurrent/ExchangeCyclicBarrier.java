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

package com.gigaspaces.internal.utils.concurrent;

import java.util.concurrent.CyclicBarrier;

@com.gigaspaces.api.InternalApi
public class ExchangeCyclicBarrier<T>
        extends CyclicBarrier implements IExchangeCyclicBarrier<T> {

    private volatile T _value;
    private volatile boolean _failed;

    public ExchangeCyclicBarrier(int parties) {
        super(parties);
    }

    public ExchangeCyclicBarrier(int parties, Runnable runnable) {
        super(parties, runnable);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.utils.concurrent.IExchangeCyclicBarrier#set(T)
     */
    public void set(T value) {
        _value = value;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.utils.concurrent.IExchangeCyclicBarrier#get()
     */
    public T get() {
        return _value;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.utils.concurrent.IExchangeCyclicBarrier#setFailed()
     */
    public void setFailed() {
        _failed = true;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.utils.concurrent.IExchangeCyclicBarrier#isFailed()
     */
    public boolean isFailed() {
        return _failed;
    }

}
