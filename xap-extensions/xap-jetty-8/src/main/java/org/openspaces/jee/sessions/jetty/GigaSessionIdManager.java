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


package org.openspaces.jee.sessions.jetty;

import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.lease.Lease;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.session.AbstractSessionManager;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.webapp.WebAppContext;

import java.util.Random;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * GigaspacesSessionIdManager
 *
 * A Jetty SessionIDManager where the in-use session ids are stored in a data grid "cloud".
 */
public class GigaSessionIdManager extends AbstractSessionIdManager {

    protected IJSpace space;

    private long lease = Lease.FOREVER;

    public GigaSessionIdManager(Server server) {
        super(server);
    }

    public GigaSessionIdManager(Server server, Random random) {
        super(server, random);
    }

    public void setSpace(IJSpace space) {
        this.space = space;
    }

    public IJSpace getSpace() {
        return space;
    }

    /**
     * Sets the lease in seconds of Session Ids written to the Space.
     */
    public void setLease(long lease) {
        this.lease = lease * 1000;
    }

    public void addSession(HttpSession session) {
        // do nothing, we use SessionData
    }

    public String getClusterId(String nodeId) {
        int dot = nodeId.lastIndexOf('.');
        return (dot > 0) ? nodeId.substring(0, dot) : nodeId;
    }

    public String getNodeId(String clusterId, HttpServletRequest request) {
        if (_workerName != null)
            return clusterId + '.' + _workerName;

        return clusterId;
    }

    public boolean idInUse(String id) {
        if (id == null)
            return false;

        try {
            SessionData sessionData = new SessionData(getClusterId(id));
            int count = space.count(sessionData, null, ReadModifiers.MATCH_BY_ID);
            if (Log.isDebugEnabled())
                Log.debug("Id [" + id + "] " + (count > 0 ? "exists" : "does not exist") + " in space");
            return count > 0;
        } catch (Exception e) {
            Log.warn("Problem checking inUse for id=" + getClusterId(id), e);
            return false;
        }
    }

    public void invalidateAll(String id) {
        //tell all contexts that may have a session object with this id to
        //get rid of them
        Handler[] contexts = _server.getChildHandlersByClass(WebAppContext.class);
        for (int i = 0; contexts != null && i < contexts.length; i++) {
            AbstractSessionManager manager = ((AbstractSessionManager) ((WebAppContext) contexts[i]).getSessionHandler().getSessionManager());
            if (manager instanceof GigaSessionManager) {
                ((GigaSessionManager) manager).invalidateSession(id);
            }
        }
    }

    public void removeSession(HttpSession session) {
        // do nothing, we reused the SessionData
    }


    /**
     * Start up the id manager.
     */
    public void doStart() {
        try {
            if (space == null) {
                initSpace();
            }
            super.doStart();
        } catch (Exception e) {
            Log.warn("Problem initialising session ids", e);
            throw new IllegalStateException(e);
        }
    }

    protected void initSpace() throws Exception {
        if (space == null) {
            throw new IllegalStateException("No space configured");
        }
    }
}
