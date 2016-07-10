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

import java.util.Map;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Java 6 (JSR 223) script executor.
 *
 * @author kimchy
 */
public class Jsr223LocalScriptExecutor extends AbstractLocalScriptExecutor<Object> {

    private ScriptEngineManager scriptEngineManager;

    public Jsr223LocalScriptExecutor() {
        scriptEngineManager = new ScriptEngineManager(Thread.currentThread().getContextClassLoader());
    }

    public Object doCompile(Script script) throws ScriptCompilationException {
        ScriptEngine scriptEngine = scriptEngineManager.getEngineByName(script.getType());
        if (scriptEngine == null) {
            throw new ScriptingException("Failed to find script engine for type [" + script.getType() + "] with Jsr223 support");
        }
        if (scriptEngine instanceof Compilable) {
            try {
                return ((Compilable) scriptEngine).compile(script.getScriptAsString());
            } catch (ScriptException e) {
                throw new ScriptCompilationException("Failed to compile script [" + script.getName() + "]: " + e.getMessage());
            }
        }
        return scriptEngine;
    }

    public Object execute(Script script, Object compiledScript, Map<String, Object> parameters) throws ScriptExecutionException {
        if (compiledScript instanceof ScriptEngine) {
            ScriptEngine scriptEngine = (ScriptEngine) compiledScript;
            Bindings bindings = scriptEngine.createBindings();
            if (parameters != null) {
                for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                    bindings.put(entry.getKey(), entry.getValue());
                }
            }
            try {
                return scriptEngine.eval(script.getScriptAsString(), bindings);
            } catch (ScriptException e) {
                throw new ScriptExecutionException("Failed to execute script [" + script.getName() + "]", e);
            }
        }
        CompiledScript cmpScript = (CompiledScript) compiledScript;
        Bindings bindings = cmpScript.getEngine().createBindings();
        if (parameters != null) {
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                bindings.put(entry.getKey(), entry.getValue());
            }
        }
        try {
            return cmpScript.eval(bindings);
        } catch (ScriptException e) {
            throw new ScriptExecutionException("Failed to execute script [" + script.getName() + "]", e);
        }
    }

    public void close(Object compiledScript) {
    }

    public boolean isThreadSafe() {
        return false;
    }
}
