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


package org.openspaces.events.polling.receive;

import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.TakeModifiers;

/**
 * Support class to perform receive operations with or without Fifo Grouping. <p>If configured to
 * use Fifo Grouping, the read/take operation will use {@link ReadModifiers#FIFO_GROUPING_POLL} /
 * {@link TakeModifiers#FIFO_GROUPING_POLL} accordingly. <ul><b>Note:</b> <li>All the handlers that
 * uses the Fifo Grouping capability should be used with a template that uses Fifo Grouping </li>
 * <li>All the handlers that uses the Fifo Grouping capability must be performed under a transaction
 * </li> </ul>
 *
 * @author yael
 * @since 9.0
 */
public abstract class AbstractFifoGroupingReceiveOperationHandler extends AbstractMemoryOnlySearchReceiveOperationHandler {

    protected boolean useFifoGrouping = false;

    /**
     * Returns whether the handler is configured to use fifo grouping or not.
     */
    public boolean isUseFifoGrouping() {
        return useFifoGrouping;
    }

    /**
     * Allows to configure the take/read operation to be performed in a Fifo Grouping manner.
     *
     * @param useFifoGrouping if true, will use {@link ReadModifiers#FIFO_GROUPING_POLL} / {@link
     *                        TakeModifiers#FIFO_GROUPING_POLL} as read/take modifiers.
     */
    public void setUseFifoGrouping(boolean useFifoGrouping) {
        this.useFifoGrouping = useFifoGrouping;
    }


}
