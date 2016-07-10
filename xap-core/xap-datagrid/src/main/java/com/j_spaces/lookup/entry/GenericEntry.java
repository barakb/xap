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

package com.j_spaces.lookup.entry;

import net.jini.lookup.entry.ServiceControlled;

abstract public class GenericEntry extends net.jini.entry.AbstractEntry implements ServiceControlled {
    private static final long serialVersionUID = -6870506314177400961L;

    abstract public net.jini.core.entry.Entry fromString(String attributeName);
}