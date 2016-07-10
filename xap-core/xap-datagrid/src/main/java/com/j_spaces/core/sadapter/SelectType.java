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

package com.j_spaces.core.sadapter;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

/**
 * Specifies types of select. <li> NEW_ENTRIES - only entries that are written under the
 * transaction. </li> <li> ALL_ENTRIES - entries that are locked under the transaction. </li> <li>
 * NEED_NOTIFY_ENTRIES - entries that need notification. </li> <li> TAKEN_ENTRIES - entries that are
 * taken under the transaction. </li>
 */
@com.gigaspaces.api.InternalApi
public class SelectType {
    public final static int NEW_ENTRIES = 1;
    public final static int ALL_ENTRIES = 2;
    public final static int NEED_NOTIFY_ENTRIES = 3;
    public final static int TAKEN_ENTRIES = 4;
    public final static int ALL_FIFO_ENTRIES = 5;

}