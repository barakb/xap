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
import javax.jms.Session;

/**
 * The JMS layer uses this exception to notify to the JMS client about transaction abortion due to a
 * space failure. The JMS layer sends this exception to the <code>ExceptionListener</code> of the
 * <code>Connection</code>. The JMS client has to register an <code>ExceptionListener</code> on that
 * <code>Connection</code> to receive this notification. The results of the space failure are: If
 * the session is in CLIENT_ACKNOWLEDGE mode unacknowledged messages are recovered. If the session
 * is transacted unacknowledged messages are recovered and produced messages are lost. The client
 * has to respond to the space failure accordingly. To get the session that threw the exception
 * simply call <code>getSession</code>.
 *
 * @author shaiw
 * @version 6.0
 */
public class SpaceLostException extends JMSException {
    private static final long serialVersionUID = -8244754842592025705L;

    private Session session;

    SpaceLostException(String message, Exception cause, Session session) {
        super(message);
        this.setLinkedException(cause);
        this.session = session;
    }

    /**
     * Returns the session that threw the exception.
     *
     * @return the session that threw the exception.
     */
    public Session getSession() {
        return session;
    }
}
