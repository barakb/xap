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

package com.gigaspaces.executor;

import com.j_spaces.core.IJSpace;

import net.jini.core.transaction.Transaction;

import java.io.Serializable;

/**
 * the basic interface for the execution service
 *
 * @author asy ronen
 * @since 6.5
 */
public interface SpaceTask<T extends Serializable> extends Serializable {
    T execute(IJSpace space, Transaction tx) throws Exception;
}
