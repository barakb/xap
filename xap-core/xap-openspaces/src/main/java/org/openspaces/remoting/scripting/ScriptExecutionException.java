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


package org.openspaces.remoting.scripting;

/**
 * An exception indicating failure to execute the script.
 *
 * @author kimchy
 */
public class ScriptExecutionException extends ScriptingException {

    private static final long serialVersionUID = -8942791771917805941L;

    public ScriptExecutionException(String msg) {
        super(msg);
    }

    public ScriptExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
