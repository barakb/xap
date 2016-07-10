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

import javax.jms.JMSException;
import javax.jms.QueueSession;
import javax.jms.XAQueueSession;

/**
 * GigaSpaces implemention of the <code>javax.jms.XAQueueSession</code> interface.
 */
public class GSXAQueueSessionImpl
        extends GSXASessionImpl
        implements XAQueueSession, QueueSession {
    /**
     * Creates an instance of GSXAQueueSessionImpl.
     *
     * @param connection parent connection
     * @throws JMSException if failed to create the session.
     */
    public GSXAQueueSessionImpl(GSXAConnectionImpl connection)
            throws JMSException {
        super(connection);
    }

    /**
     * @see XAQueueSession#getQueueSession()
     */
    public QueueSession getQueueSession() throws JMSException {
        return this;
    }
}//end of class