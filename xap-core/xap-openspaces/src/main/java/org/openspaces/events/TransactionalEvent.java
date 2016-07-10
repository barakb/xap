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


package org.openspaces.events;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an event container configured using annotations ({@link org.openspaces.events.polling.Polling}
 * or {@link org.openspaces.events.notify.Notify} as transactional.
 *
 * @author kimchy
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransactionalEvent {

    /**
     * The transaction propagation type. <p>Defaults to {@link org.springframework.transaction.annotation.Propagation#REQUIRED}.
     */
    Propagation propagation() default org.springframework.transaction.annotation.Propagation.REQUIRED;

    /**
     * The transaction isolation level. <p>Defaults to {@link org.springframework.transaction.annotation.Isolation#DEFAULT}.
     */
    Isolation isolation() default org.springframework.transaction.annotation.Isolation.DEFAULT;

    /**
     * The timeout for this transaction (in seconds). <p>Defaults to the default timeout of the
     * underlying transaction system.
     */
    int timeout() default TransactionDefinition.TIMEOUT_DEFAULT;

    /**
     * A reference to the actual transaction manager. Only needed if there is more than one
     * transaction manager in the context. If there is only one, will use it by default.
     */
    String transactionManager() default "";
}
