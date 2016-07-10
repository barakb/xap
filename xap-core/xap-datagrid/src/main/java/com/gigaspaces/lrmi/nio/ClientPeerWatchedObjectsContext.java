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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.lrmi.nio.watchdog.Watchdog;
import com.gigaspaces.lrmi.nio.watchdog.Watchdog.Group;

/**
 * Holds cpeer watched objects
 *
 * At any given moment, at most 1 object is being watched. calling watchXXXX will stop the watch for
 * the currently watched object unless the currently watched object is NONE or the call to watchXXX
 * resulted in no state change (that is, XXXX is the currently watched object)
 *
 * @author Dan Kilman
 */
@com.gigaspaces.api.InternalApi
public class ClientPeerWatchedObjectsContext {
    private static enum WatchedObject {NONE, IDLE, REQUEST, RESPONSE}

    private final Watchdog.WatchedObject _watchedRequestObject;
    private final Watchdog.WatchedObject _watchedResponseObject;
    private final Watchdog.WatchedObject _watchedIdle;

    private WatchedObject _currentlyWatchedObject = WatchedObject.NONE;

    public ClientPeerWatchedObjectsContext(CPeer cpeer) {
        _watchedRequestObject = Watchdog.getGroup(Group.REQUEST_GROUP).addRequestWatch(cpeer.getChannel(), cpeer);
        _watchedResponseObject = Watchdog.getGroup(Group.RESPONSE_GROUP).addResponseWatch(cpeer.getChannel(), cpeer);
        _watchedIdle = Watchdog.getGroup(Group.IDLE_GROUP).addIdleWatch(cpeer);
    }

    public void close() {
        Watchdog.getGroup(Group.REQUEST_GROUP).removeWatch(_watchedRequestObject);
        Watchdog.getGroup(Group.RESPONSE_GROUP).removeWatch(_watchedResponseObject);
        Watchdog.getGroup(Group.IDLE_GROUP).removeWatch(_watchedIdle);
    }

    public void watchIdle() {
        watch(WatchedObject.IDLE, "");
    }

    public void watchRequest(String monitoringId) {
        watch(WatchedObject.REQUEST, monitoringId);
    }

    public void watchResponse(String monitoringId) {
        watch(WatchedObject.RESPONSE, monitoringId);
    }

    public void watchNone() {
        watch(WatchedObject.NONE, "");
    }

    public boolean requestWatchHasException() {
        return _watchedRequestObject.getException() != null;
    }

    public Exception getAndClearRequestWatchException() {
        Exception result = _watchedRequestObject.getException();
        _watchedRequestObject.setException(null);
        return result;
    }

    public boolean responseWatchHasException() {
        return _watchedResponseObject.getException() != null;
    }

    public Exception getAndClearResponseWatchException() {
        Exception result = _watchedResponseObject.getException();
        _watchedResponseObject.setException(null);
        return result;
    }

    private void watch(WatchedObject toWatch, String monitoringId) {
        WatchedObject currentlyWatched = _currentlyWatchedObject;

        if (currentlyWatched == toWatch)
            return;

        switch (currentlyWatched) {
            case IDLE:
                _watchedIdle.stopWatch();
                _watchedIdle.setMonitoringId(null);
                break;
            case REQUEST:
                _watchedRequestObject.stopWatch();
                _watchedRequestObject.setMonitoringId(null);
                break;
            case RESPONSE:
                _watchedResponseObject.stopWatch();
                _watchedResponseObject.incrementVersion();
                _watchedResponseObject.setMonitoringId(null);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unsupported state: " + currentlyWatched);
        }

        switch (toWatch) {
            case IDLE:
                _watchedIdle.setMonitoringId(monitoringId);
                _watchedIdle.startWatch();
                break;
            case REQUEST:
                _watchedRequestObject.setMonitoringId(monitoringId);
                _watchedRequestObject.startWatch();
                break;
            case RESPONSE:
                _watchedResponseObject.setMonitoringId(monitoringId);
                _watchedResponseObject.startWatch();
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unsupported state: " + toWatch);
        }

        _currentlyWatchedObject = toWatch;
    }

}
