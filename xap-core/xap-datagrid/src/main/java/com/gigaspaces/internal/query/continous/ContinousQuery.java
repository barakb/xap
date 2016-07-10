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

package com.gigaspaces.internal.query.continous;

import com.gigaspaces.events.AbstractDataEventSession;
import com.gigaspaces.events.DataEventSession;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Niv Ingberg
 * @since 8.0.3
 */
@com.gigaspaces.api.InternalApi
public class ContinousQuery implements RemoteEventListener, BatchRemoteEventListener {
    private final IJSpace _spaceProxy;
    private final Object _query;
    private final ContinousQueryListener _listener;
    private final ContinousQueryConfig _config;
    private final AbstractDataEventSession _eventSession;
    private final Queue<EntryArrivedRemoteEvent> _pendingEvents;
    private final EventRegistration _eventRegistration;
    private Thread _eventsProcessorThread;
    private boolean _doneFirstStage;
    private boolean _closed;

    public ContinousQuery(ISpaceProxy spaceProxy, ITemplatePacket query, ContinousQueryListener listener,
                          ContinousQueryConfig config, DataEventSession eventSession)
            throws RemoteException {
        this._spaceProxy = spaceProxy;
        this._query = query;
        this._listener = listener;
        this._config = config;
        this._eventSession = (AbstractDataEventSession) eventSession;
        this._pendingEvents = new LinkedList<EntryArrivedRemoteEvent>();

        // Register for notifications:
        NotifyInfo notifyInfo = _eventSession.createNotifyInfo(this, _config.getNotifyActionType());
        notifyInfo.setReturnOnlyUids(_config.isReturnOnlyUid());

        try {
            this._eventRegistration = this._eventSession.addListener(_query, Lease.FOREVER, notifyInfo);

            readExistingEntries();
        } catch (TransactionException e) {
            throw new IllegalStateException("Transaction exception occurred but transactions are not used.");
        } catch (UnusableEntryException e) {
            throw new ProxyInternalSpaceException("Failed to read existing entries", e);
        }

        synchronized (_pendingEvents) {
            if (!_pendingEvents.isEmpty()) {
                // Start pending events processor:
                this._eventsProcessorThread = new GSThread(new PendingEventsProcessor(), "ContinuousQueryEventsProcessor");
                this._eventsProcessorThread.setDaemon(true);
                this._eventsProcessorThread.start();
            } else
                _doneFirstStage = true;
        }
    }

    private void readExistingEntries()
            throws RemoteException, UnusableEntryException, TransactionException {
        // TODO: Switch to GSIterator when security bug is fixed.
        Object[] results = _spaceProxy.readMultiple(_query, null, Integer.MAX_VALUE, _config.getReadModifiers());
        for (Object result : results)
            _listener.onExisting((IEntryPacket) result);
    }

    public synchronized void close()
            throws RemoteException {
        if (_closed)
            return;

        _closed = true;
        _eventSession.removeListener(_eventRegistration);
    }

    @Override
    public void notify(RemoteEvent remoteEvent)
            throws UnknownEventException, RemoteException {
        if (!_doneFirstStage) {
            synchronized (_pendingEvents) {
                if (!_doneFirstStage) {
                    _pendingEvents.add((EntryArrivedRemoteEvent) remoteEvent);
                    return;
                }
            }
        }
        _listener.onEvent((EntryArrivedRemoteEvent) remoteEvent);
    }

    @Override
    public void notifyBatch(BatchRemoteEvent theEvents)
            throws UnknownEventException, RemoteException {
        if (!_doneFirstStage) {
            synchronized (_pendingEvents) {
                if (!_doneFirstStage) {
                    for (RemoteEvent remoteEvent : theEvents.getEvents()) {
                        _pendingEvents.add((EntryArrivedRemoteEvent) remoteEvent);
                    }
                    return;
                }
            }
        }
        for (RemoteEvent remoteEvent : theEvents.getEvents()) {
            _listener.onEvent((EntryArrivedRemoteEvent) remoteEvent);
        }
    }

    private class PendingEventsProcessor implements Runnable {
        @Override
        public void run() {
            // Process pending events:
            while (true && !_closed) {
                synchronized (_pendingEvents) {
                    EntryArrivedRemoteEvent event = _pendingEvents.poll();
                    if (event == null) {
                        _doneFirstStage = true;
                        return;
                    }
                    _listener.onEvent(event);
                }
            }
        }
    }
}
