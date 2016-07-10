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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.remoting.scripting.cache.CompiledScriptCache;
import org.openspaces.remoting.scripting.cache.LRUNonThreadSafeCompiledScriptCache;
import org.openspaces.remoting.scripting.cache.LRUThreadSafeCompiledScriptCache;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.ClassUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * A Default "server" side script executor. Can execute {@link org.openspaces.remoting.scripting.Script}s
 * based on a set of registered {@link org.openspaces.remoting.scripting.LocalScriptExecutor}s.
 *
 * <p>Will automatically register <code>groovy</code> ({@link org.openspaces.remoting.scripting.GroovyLocalScriptExecutor})
 * if it exists within the classpath. Will also automatically register <code>ruby</code> ({@link
 * org.openspaces.remoting.scripting.JRubyLocalScriptExecutor}) if the jruby jars exists within the
 * classpath.
 *
 * <p>If working under Java 6, or adding JSR 223 jars into the classpath, will use its scripting
 * support as a fall-back if no local script executors are found for a given type. The JSR allows
 * for a unified API on top of different scripting libraries with pluggable types.
 *
 * <p>The executor will automatically add the Spring application context as a parameter to the
 * script under the name <code>applicationContext</code> allowing to get any beans defined on the
 * "server side". Since <code>GigaSpace</code> instances are often used within the script, it will
 * automatically add all the different <code>GigaSpace</code> instances defined within the
 * application context under their respective bean names.
 *
 * <p>Another parameter that will be passed to the script is the {@link
 * org.openspaces.core.cluster.ClusterInfo} allowing the script to be "aware" of which cluster
 * instance it is executed on, and the cluster size.
 *
 * <p>This executor also supports caching of compiled scripts using the {@link
 * org.openspaces.remoting.scripting.cache.CompiledScriptCache} abstraction. There are two special
 * caches, one for compiled scripts that are thread safe (the same compiled script can be executed
 * by several threads) and one for compiled scripts that are not thread safe (the same compiled
 * scripts can only be executed by a single thread). Note, caching is done based on the script name
 * ({@link Script#getName()}, this means that changed scripts should change their name in order to
 * recompile them and insert them into the cache.
 *
 * @author kimchy
 */
public class DefaultScriptingExecutor implements ScriptingExecutor, ApplicationContextAware, InitializingBean,
        ApplicationListener, ClusterInfoAware {

    private static final Log logger = LogFactory.getLog(DefaultScriptingExecutor.class);

    public static final String APPLICATION_CONTEXT_KEY = "applicationContext";

    public static final String CLUSTER_INFO_KEY = "clusterInfo";

    public static final String GROOVY_LOCAL_EXECUTOR_TYPE = "groovy";

    public static final String JRUBY_LOCAL_EXECUTOR_TYPE = "ruby";

    private ApplicationContext applicationContext;

    private Map<String, Object> parameters;

    private Map<String, Class<?>> parameterTypes;

    private Map<String, LocalScriptExecutor> executors = new HashMap<String, LocalScriptExecutor>();

    private LocalScriptExecutor jsr223Executor;

    private CompiledScriptCache threadSafeCompiledScriptCache;

    private CompiledScriptCache nonThreadSafeCopmiledScriptCache;

    private final Map<String, GigaSpace> gigaSpacesBeans = new HashMap<String, GigaSpace>();

    private ClusterInfo clusterInfo;

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    /**
     * Sets parameters that will be added to each script. The key of the map is the parameter name
     * (a String) and the value if a free Object as the parameter value.
     */
    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    /**
     * Sets parameter types for parameters that will be added to each script. The key of the map is
     * the parameter name (a String) and the value is a class representing that static type for the
     * key name.
     */
    public void setParameterTypes(Map<String, Class<?>> parameterTypes) {
        this.parameterTypes = parameterTypes;
    }

    public void setExecutors(Map<String, LocalScriptExecutor> executors) {
        this.executors = executors;
    }

    /**
     * Sets a compiled script cache for compiled scripts taht are thread safe (the same script can
     * be executed by different threads). Defaults to {@link LRUThreadSafeCompiledScriptCache}.
     *
     * @see LocalScriptExecutor#isThreadSafe()
     */
    public void setThreadSafeCompiledScriptCache(CompiledScriptCache threadSafeCompiledScriptCache) {
        this.threadSafeCompiledScriptCache = threadSafeCompiledScriptCache;
    }

    /**
     * Sets a compiled script cache for compiled scripts taht are not thread safe (the same script
     * can not be executed by different threads). Defaults to {@link LRUNonThreadSafeCompiledScriptCache}.
     *
     * @see LocalScriptExecutor#isThreadSafe()
     */
    public void setNonThreadSafeCopmiledScriptCache(CompiledScriptCache nonThreadSafeCopmiledScriptCache) {
        this.nonThreadSafeCopmiledScriptCache = nonThreadSafeCopmiledScriptCache;
    }

    public void afterPropertiesSet() throws Exception {
        if (!executors.containsKey(GROOVY_LOCAL_EXECUTOR_TYPE)) {
            // try and create the groovy executor if it exists in the class path
            try {
                LocalScriptExecutor groovyExecutor = (LocalScriptExecutor) ClassUtils.forName("org.openspaces.remoting.scripting.GroovyLocalScriptExecutor", Thread.currentThread().getContextClassLoader()).newInstance();
                executors.put(GROOVY_LOCAL_EXECUTOR_TYPE, groovyExecutor);
                if (logger.isDebugEnabled()) {
                    logger.debug("Groovy detected in the classpath, adding it as a local executor under the [" + GROOVY_LOCAL_EXECUTOR_TYPE + "] type");
                }
            } catch (Error e) {
                // no grovy in the classpath, don't register it
            }
        }
        if (!executors.containsKey(JRUBY_LOCAL_EXECUTOR_TYPE)) {
            // try and create the groovy executor if it exists in the class path
            try {
                LocalScriptExecutor jrubyExecutor = (LocalScriptExecutor) ClassUtils.forName("org.openspaces.remoting.scripting.JRubyLocalScriptExecutor", Thread.currentThread().getContextClassLoader()).newInstance();
                executors.put(JRUBY_LOCAL_EXECUTOR_TYPE, jrubyExecutor);
                if (logger.isDebugEnabled()) {
                    logger.debug("JRuby detected in the classpath, adding it as a local executor under the [" + JRUBY_LOCAL_EXECUTOR_TYPE + "] type");
                }
            } catch (Error e) {
                // no jruby in the classpath, don't register it
            }
        }
        try {
            jsr223Executor = (LocalScriptExecutor) ClassUtils.forName("org.openspaces.remoting.scripting.Jsr223LocalScriptExecutor", Thread.currentThread().getContextClassLoader()).newInstance();
            if (logger.isDebugEnabled()) {
                logger.debug("Java 6 (JSR 223) detected in the classpath, adding it as a default executor");
            }
        } catch (Error e) {
            // not working with Java 6, or JSR 223 jars are not included
        }

        // validate that we have at least one local script executor
        if (executors.size() == 0 && jsr223Executor == null) {
            logger.info("No local script executors are configured or automatically detected");
        }

        if (threadSafeCompiledScriptCache == null) {
            threadSafeCompiledScriptCache = new LRUThreadSafeCompiledScriptCache();
        }
        if (nonThreadSafeCopmiledScriptCache == null) {
            nonThreadSafeCopmiledScriptCache = new LRUNonThreadSafeCompiledScriptCache();
        }
    }

    /**
     * On applicaiton context refresh event get all the GigaSpace beans and put them in a map that
     * will later be appeneded to any script parameters.
     */
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (applicationEvent instanceof ContextRefreshedEvent) {
            Map gigaBeans = applicationContext.getBeansOfType(GigaSpace.class);
            if (gigaBeans != null) {
                for (Iterator it = gigaBeans.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) it.next();
                    gigaSpacesBeans.put((String) entry.getKey(), (GigaSpace) entry.getValue());
                }
            }
        }
    }

    public Object execute(Script script) throws ScriptingException {
        if (script.getType() == null) {
            throw new IllegalArgumentException("Script must contain type");
        }
        LocalScriptExecutor localScriptExecutor = executors.get(script.getType());
        // if we fail to find one specific for that type, fail (if possible) to JSR 223
        if (localScriptExecutor == null) {
            localScriptExecutor = jsr223Executor;
            if (localScriptExecutor == null) {
                throw new ScriptingException("Failed to find executor for type [" + script.getType() + "]");
            }
        }

        Map<String, Object> scriptParams = new HashMap<String, Object>();
        if (parameters != null) {
            scriptParams.putAll(parameters);
        }
        scriptParams.putAll(gigaSpacesBeans);
        scriptParams.put(APPLICATION_CONTEXT_KEY, applicationContext);
        scriptParams.put(CLUSTER_INFO_KEY, clusterInfo);
        if (script.getParameters() != null) {
            scriptParams.putAll(script.getParameters());
        }

        if (script instanceof TypedScript) {
            TypedScript typedScript = (TypedScript) script;
            Map<String, Class<?>> scriptParameterTypes = typedScript.getParameterTypes();
            if (scriptParameterTypes == null) {
                throw new ScriptingException("parameterTypes cannot be null for typed script");
            }
            if (parameterTypes != null) {
                for (Map.Entry<String, Class<?>> entry : parameterTypes.entrySet()) {
                    if (scriptParameterTypes.containsKey(entry.getKey())) {
                        continue;
                    }
                    scriptParameterTypes.put(entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<String, Object> entry : scriptParams.entrySet()) {
                if (scriptParameterTypes.containsKey(entry.getKey())) {
                    continue;
                }
                Class<?> paramType = entry.getValue() != null ? entry.getValue().getClass() : Object.class;
                scriptParameterTypes.put(entry.getKey(), paramType);
            }
        }

        Object compiledScript;
        if (script.shouldCache()) {
            if (localScriptExecutor.isThreadSafe()) {
                compiledScript = threadSafeCompiledScriptCache.get(script.getName(), localScriptExecutor, script);
            } else {
                compiledScript = nonThreadSafeCopmiledScriptCache.get(script.getName(), localScriptExecutor, script);
            }
        } else {
            compiledScript = localScriptExecutor.compile(script);
        }

        try {
            return localScriptExecutor.execute(script, compiledScript, scriptParams);
        } finally {
            if (script.shouldCache()) {
                if (localScriptExecutor.isThreadSafe()) {
                    threadSafeCompiledScriptCache.put(script.getName(), compiledScript, localScriptExecutor);
                } else {
                    nonThreadSafeCopmiledScriptCache.put(script.getName(), compiledScript, localScriptExecutor);
                }
            } else {
                localScriptExecutor.close(compiledScript);
            }
        }
    }

    public Future asyncExecute(Script script) throws ScriptingException {
        return null;
    }
}
