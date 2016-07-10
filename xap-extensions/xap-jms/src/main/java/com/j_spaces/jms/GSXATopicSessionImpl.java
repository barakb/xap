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
import javax.jms.TopicSession;
import javax.jms.XATopicSession;

/**
 * GigaSpaces implemention of the <code>javax.jms.XATopicSession</code> interface.
 */
public class GSXATopicSessionImpl
        extends GSXASessionImpl
        implements XATopicSession, TopicSession {
    /**
     * @param connection parent connection
     */
    public GSXATopicSessionImpl(GSXAConnectionImpl connection)
            throws JMSException {
        super(connection);
    }

    /**
     * @see XATopicSession#getTopicSession()
     */
    public TopicSession getTopicSession() throws JMSException {
        return this;
    }
}//end of class
