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


package org.openspaces.core.transaction.manager;

import org.springframework.transaction.PlatformTransactionManager;

/**
 * An extension to Spring {@link org.springframework.transaction.PlatformTransactionManager} that
 * holds the Jini transactional context. The transactional context is the context the Jini
 * transaction is bounded under (usually using Spring synchronization which is based on thread
 * local).
 *
 * @author kimchy
 */
public interface JiniPlatformTransactionManager extends PlatformTransactionManager {

    /**
     * Returns the transactional context the jini transaction is bounded under (usually using Spring
     * synchronization which is based on thread local).
     */
    Object getTransactionalContext();

    String getBeanName();
}
