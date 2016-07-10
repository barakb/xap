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
package com.sun.jini.reggie.sender;

import com.sun.jini.reggie.RegistrarEvent;

import net.jini.core.lookup.ServiceID;

import java.util.List;

/**
 * Created by Barak Bar Orion 7/7/15.
 */
@com.gigaspaces.api.InternalApi
public class EventsCompressor {
    public static void compress(List<RegistrarEvent> events, RegistrarEvent event) {
        if (events.isEmpty()) {
            events.add(event);
            return;
        }
        int index = findEventWithServiceId(event.getServiceID(), events);
        if (-1 < index) {
            RegistrarEvent old = events.get(index);
            int oldTransition = old.getTransition();
            int currentTransition = event.getTransition();
            if (oldTransition == 1) {
                if (currentTransition == 1) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else if (currentTransition == 2) {
                    events.remove(index);
                } else if (currentTransition == 4) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                }
            } else if (oldTransition == 2) {
                if (currentTransition == 1) {
                    events.remove(index);
                } else if (currentTransition == 2) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else if (currentTransition == 4) {
                    event.setTransition(4);
                    events.set(index, event);
                } else {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                }

            } else if (oldTransition == 4) {
                if (currentTransition == 1) {
                    events.set(index, event);
                } else if (currentTransition == 2) {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                } else if (currentTransition == 4) {
                    events.set(index, event);
                } else {
                    throw new IllegalStateException("compressing event " + event + " onto " + events);
                }
            } else {
                throw new IllegalStateException("compressing event " + event + " onto " + events);
            }
        }

    }

    private static int findEventWithServiceId(ServiceID serviceID, List<RegistrarEvent> events) {
        int size = events.size();
        for (int i = size - 1; -1 < i; --i) {
            RegistrarEvent candidate = events.get(i);
            if (candidate.getServiceID().equals(serviceID)) {
                return i;
            }
        }
        return -1;
    }

// this is doc !
//    public static String translate(int value) {
//        if(value == 1){
//            return "MATCH_NOMATCH";
//        }else if(value == 2){
//            return "NOMATCH_MATCH";
//        }else if(value== 4){
//            return "MATCH_MATCH";
//        }
//        return String.valueOf(value);
//    }
}
