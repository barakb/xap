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

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.time.SystemTime;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * A simple class that can trace events with a predefined limited record length
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class EventsTracer<T> {
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    public static class Event<T> {
        private final T _event;
        private final long _timeStamp;

        public Event(T event, long timeStamp) {
            _event = event;
            _timeStamp = timeStamp;
        }

        public T getEvent() {
            return _event;
        }

        public long getTimeStamp() {
            return _timeStamp;
        }

        @Override
        public String toString() {
            return SIMPLE_DATE_FORMAT.format(new Date(_timeStamp)) + ":" + StringUtils.NEW_LINE + _event;
        }
    }


    private final Event<T>[] _events;
    private final int _length;

    private int _lastEventIndex = -1;

    @SuppressWarnings("unchecked")
    public EventsTracer(int length) {
        _length = length;
        _events = new Event[length];
    }

    public synchronized void logEvent(T event) {
        _lastEventIndex = (_lastEventIndex + 1) % _length;
        _events[_lastEventIndex] = new Event<T>(event, SystemTime.timeMillis());
    }

    public synchronized Event<T> getLastEvent() {
        //No events yet
        if (_lastEventIndex == -1)
            return null;

        return _events[_lastEventIndex];
    }

    public synchronized Iterable<Event<T>> getAccesendingEvents() {
        LinkedList<Event<T>> descendingEventsList = createDescendingEventsList();
        LinkedList<Event<T>> result = new LinkedList<Event<T>>();
        ListIterator<Event<T>> listIterator = descendingEventsList.listIterator(descendingEventsList.size());
        while (listIterator.hasPrevious())
            result.add(listIterator.previous());

        return result;
    }

    public synchronized Iterable<Event<T>> getDescendingEvents() {
        return createDescendingEventsList();
    }

    public String outputDescendingEvents() {
        StringBuilder sb = new StringBuilder();
        sb.append("------------------");
        sb.append(StringUtils.NEW_LINE);
        Iterable<Event<T>> descendingEvents = getDescendingEvents();
        for (Event<T> event : descendingEvents) {
            sb.append(event);
            sb.append(StringUtils.NEW_LINE);
            sb.append("------------------");
            sb.append(StringUtils.NEW_LINE);
        }
        return sb.toString();
    }

    private LinkedList<Event<T>> createDescendingEventsList() {
        LinkedList<Event<T>> result = new LinkedList<Event<T>>();
        for (int i = 0; i < _length; i++) {
            int positionWithoutOffset = _lastEventIndex - i;
            int index = positionWithoutOffset < 0 ? positionWithoutOffset + _length : positionWithoutOffset;
            Event<T> event = _events[index];
            if (event == null)
                break;
            result.add(event);
        }
        return result;
    }


    public static void main(String[] args) {
        EventsTracer<Integer> tracer = new EventsTracer<Integer>(3);
        System.out.println("\n" + tracer.getLastEvent());
        Iterable<Event<Integer>> accesendingEvents = tracer.getAccesendingEvents();
        System.out.println("ascending 1");
        for (Event<Integer> integer : accesendingEvents) {
            System.out.println(integer);
        }

        Iterable<Event<Integer>> descendingEvents = tracer.getDescendingEvents();
        System.out.println("descendingEvents 1");
        for (Event<Integer> integer : descendingEvents) {
            System.out.println(integer);
        }

        tracer.logEvent(1);

        System.out.println("\n" + tracer.getLastEvent());
        accesendingEvents = tracer.getAccesendingEvents();
        System.out.println("ascending 2");
        for (Event<Integer> integer : accesendingEvents) {
            System.out.println(integer);
        }

        descendingEvents = tracer.getDescendingEvents();
        System.out.println("descendingEvents 2");
        for (Event<Integer> integer : descendingEvents) {
            System.out.println(integer);
        }

        tracer.logEvent(2);

        System.out.println("\n" + tracer.getLastEvent());
        accesendingEvents = tracer.getAccesendingEvents();
        System.out.println("ascending 3");
        for (Event<Integer> integer : accesendingEvents) {
            System.out.println(integer);
        }

        descendingEvents = tracer.getDescendingEvents();
        System.out.println("descendingEvents 3");
        for (Event<Integer> integer : descendingEvents) {
            System.out.println(integer);
        }

        tracer.logEvent(3);

        System.out.println("\n" + tracer.getLastEvent());
        accesendingEvents = tracer.getAccesendingEvents();
        System.out.println("ascending 3");
        for (Event<Integer> integer : accesendingEvents) {
            System.out.println(integer);
        }

        descendingEvents = tracer.getDescendingEvents();
        System.out.println("descendingEvents 3");
        for (Event<Integer> integer : descendingEvents) {
            System.out.println(integer);
        }

        tracer.logEvent(4);

        System.out.println("\n" + tracer.getLastEvent());
        accesendingEvents = tracer.getAccesendingEvents();
        System.out.println("ascending 3");
        for (Event<Integer> integer : accesendingEvents) {
            System.out.println(integer);
        }

        descendingEvents = tracer.getDescendingEvents();
        System.out.println("descendingEvents 3");
        for (Event<Integer> integer : descendingEvents) {
            System.out.println(integer);
        }
    }
}
