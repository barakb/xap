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

/*
 * Created on 20/04/2004
 *
 * @author		Gershon Diner
 * Title:        	The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:    GigaSpaces Technologies Ltd.
 * @version 	 4.0
 */
package com.j_spaces.jms;

import com.j_spaces.core.client.IReplicatable;
import com.j_spaces.core.client.MetaDataEntry;

/**
 * A MetaDataEntry extenasion, that is used when a single Session is required. After the
 * connection.close() the JMSSessionDataEntry entry is clear()'ed from space.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class JMSSessionDataEntry extends MetaDataEntry implements IReplicatable {
    private static final long serialVersionUID = 3218833138737448842L;

    public Integer m_ackMode;
    public Boolean m_isQueue;
    public Boolean m_isTransacted;
    public Boolean m_isDurable;
    public String m_sessionId;

    public JMSSessionDataEntry() {
    }

    @Override
    public String toString() {
        return "JMSSessionDataEntry | m_sessionId=" +
                " |ackMode=" + m_ackMode +
                " | isQueue=" + m_isQueue +
                " | isTransacted=" + m_isTransacted +
                " | isDurable=" + m_isDurable;
    }
}
