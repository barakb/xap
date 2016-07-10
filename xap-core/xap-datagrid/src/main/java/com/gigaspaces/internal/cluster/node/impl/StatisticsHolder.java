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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.time.SystemTime;

import java.util.concurrent.TimeUnit;

@com.gigaspaces.api.InternalApi
public class StatisticsHolder<T> {
    private final int _samplesCount;
    private final long[] _timeStamps;
    private final T[] _samples;

    private int _startIndex;
    private final T _defaultValue;

    @SuppressWarnings("unchecked")
    public StatisticsHolder(int samplesCount, T defaultValue) {
        _samplesCount = samplesCount;
        _defaultValue = defaultValue;
        _timeStamps = new long[_samplesCount];
        _samples = (T[]) new Object[_samplesCount];
    }

    public synchronized void addSample(long timeStamp, T sample) {
        _timeStamps[_startIndex] = timeStamp;
        _samples[_startIndex] = sample;

        _startIndex = (_startIndex + 1) % _samplesCount;
    }

    public synchronized T getLastSample() {
        int lastSampleIndex = _startIndex - 1;
        if (lastSampleIndex < 0)
            lastSampleIndex = _samplesCount - 1;
        return sampleAt(lastSampleIndex);
    }

    public synchronized long getLastTimeStamp() {
        int lastSampleIndex = _startIndex - 1;
        if (lastSampleIndex < 0)
            lastSampleIndex = _samplesCount - 1;
        return _timeStamps[lastSampleIndex];
    }

    public synchronized T getSampleBefore(long timeBefore, TimeUnit unit) {
        long currentTime = SystemTime.timeMillis();
        long requestedTime = currentTime - unit.toMillis(timeBefore);

        int requestedIndex = _startIndex;
        for (int i = 0; i < _samplesCount; ++i) {
            int index = (_startIndex - 1) - i;
            if (index < 0)
                index += _samplesCount;
            if (_timeStamps[index] <= requestedTime) {
                requestedIndex = index;
                break;
            }
        }
        return sampleAt(requestedIndex);
    }

    private T sampleAt(int requestedIndex) {
        T sample = _samples[requestedIndex];
        if (sample == null)
            return _defaultValue;
        return sample;
    }

    public static void main(String[] args) throws InterruptedException {
        StatisticsHolder<Integer> statisticsHolder = new StatisticsHolder<Integer>(2, 0);
        int lastSample = statisticsHolder.getLastSample();
        System.out.println("sample " + lastSample);
        long lastTimeStamp = statisticsHolder.getLastTimeStamp();
        System.out.println("timeStamp " + lastTimeStamp);
        lastSample = statisticsHolder.getSampleBefore(3, TimeUnit.SECONDS);
        System.out.println("sample " + lastSample);

        statisticsHolder.addSample(SystemTime.timeMillis(), 5);

        lastSample = statisticsHolder.getLastSample();
        System.out.println("sample 1 " + lastSample);
        lastTimeStamp = statisticsHolder.getLastTimeStamp();
        System.out.println("timeStamp " + lastTimeStamp);
        lastSample = statisticsHolder.getSampleBefore(3, TimeUnit.SECONDS);
        System.out.println("sample 2 " + lastSample);

        Thread.sleep(1000);

        statisticsHolder.addSample(SystemTime.timeMillis(), 15);

        lastSample = statisticsHolder.getLastSample();
        System.out.println("sample 3 " + lastSample);
        lastTimeStamp = statisticsHolder.getLastTimeStamp();
        System.out.println("timeStamp " + lastTimeStamp);
        lastSample = statisticsHolder.getSampleBefore(3, TimeUnit.SECONDS);
        System.out.println("sample 4 " + lastSample);

        Thread.sleep(2000);
        lastSample = statisticsHolder.getSampleBefore(3, TimeUnit.SECONDS);
        System.out.println("sample 5 " + lastSample);
        statisticsHolder.addSample(SystemTime.timeMillis(), 20);

        Thread.sleep(2000);
        lastSample = statisticsHolder.getSampleBefore(3, TimeUnit.SECONDS);
        System.out.println("sample 6 " + lastSample);

        Thread.sleep(2000);
        lastSample = statisticsHolder.getSampleBefore(3, TimeUnit.SECONDS);
        System.out.println("sample 7 " + lastSample);

        lastSample = statisticsHolder.getLastSample();
        System.out.println("sample 8 " + lastSample);
        lastTimeStamp = statisticsHolder.getLastTimeStamp();
        System.out.println("timeStamp " + lastTimeStamp);
    }
}
