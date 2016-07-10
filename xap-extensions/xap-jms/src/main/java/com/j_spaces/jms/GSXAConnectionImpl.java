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

package com.j_spaces.jms;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;

/**
 * GigaSpaces implemention of the <code>javax.jms.XAConnection</code> interface.
 */
public class GSXAConnectionImpl
        extends GSConnectionImpl
        implements XAConnection, XATopicConnection, XAQueueConnection {
    /**
     * Create an instance of GSXAConnectionImpl.
     *
     * @param factory parent factory.
     * @throws JMSException if failed to create connection.
     */
    public GSXAConnectionImpl(GSXAConnectionFactoryImpl factory)
            throws JMSException {
        super(factory);
    }

    /**
     * @see XAConnection#createXASession()
     */
    public XASession createXASession() throws JMSException {
        if (closed) {
            throw new IllegalStateException("Forbidden call on a closed connection.");
        }

        GSXASessionImpl session = new GSXASessionImpl(this);
        if (!isStopped()) {
            session.start();
        }
        return session;
    }

    /**
     * @see XATopicConnection#createXATopicSession()
     */
    public XATopicSession createXATopicSession() throws JMSException {
        if (closed) {
            throw new IllegalStateException("Forbidden call on a closed connection.");
        }

        GSXATopicSessionImpl session = new GSXATopicSessionImpl(this);
        if (!isStopped()) {
            session.start();
        }
        return session;
    }

    /**
     * @see XAQueueConnection#createXAQueueSession()
     */
    public XAQueueSession createXAQueueSession() throws JMSException {
        if (closed) {
            throw new IllegalStateException("Forbidden call on a closed connection.");
        }

        GSXAQueueSessionImpl session = new GSXAQueueSessionImpl(this);
        if (!isStopped()) {
            session.start();
        }
        return session;
    }
}
