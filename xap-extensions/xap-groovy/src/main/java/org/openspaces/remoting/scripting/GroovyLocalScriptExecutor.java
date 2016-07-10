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

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.util.Map;

/**
 * Groovy local script executor.
 *
 * @author kimchy
 */
public class GroovyLocalScriptExecutor extends AbstractLocalScriptExecutor<groovy.lang.Script> {

    private GroovyShell groovyShell;

    public GroovyLocalScriptExecutor() {
        groovyShell = new GroovyShell(Thread.currentThread().getContextClassLoader());
    }


    protected groovy.lang.Script doCompile(Script script) throws ScriptCompilationException {
        try {
            return groovyShell.parse(script.getScriptAsString());
        } catch (Exception e) {
            throw new ScriptCompilationException("Failed to compile script [" + script.getName() + "]: " + e.getMessage());
        }
    }

    public Object execute(Script script, groovy.lang.Script compiledScript, Map<String, Object> parameters) throws ScriptExecutionException {
        Binding binding = new Binding();
        if (parameters != null) {
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                binding.setVariable(entry.getKey(), entry.getValue());
            }
        }
        compiledScript.setBinding(binding);
        try {
            return compiledScript.run();
        } catch (Exception e) {
            throw new ScriptExecutionException("Failed to execute script [" + script.getName() + "]", e);
        }
    }

    public void close(groovy.lang.Script compiledScript) {
    }

    public boolean isThreadSafe() {
        return false;
    }
}
