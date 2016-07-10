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

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.FileCopyUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * A static script that uses Spring {@link Resource} and {@link ResourceLoader} to load a given
 * script (for example, from the classpath).
 *
 * @author kimchy
 */
public class StaticResourceScript extends StaticScript {

    private static final long serialVersionUID = 3171153261690141988L;

    private ResourceLoader resourceLoader = new DefaultResourceLoader();

    public StaticResourceScript() {
        super();
    }

    public StaticResourceScript(String name, String type, String resourceLocation) {
        script(resourceLocation);
        type(type);
        name(name);
    }

    public StaticScript script(String resourceLocation) {
        super.script(loadResource(resourceLoader.getResource(resourceLocation)));
        return this;
    }

    private String loadResource(Resource resource) throws ScriptExecutionException {
        try {
            return FileCopyUtils.copyToString(new BufferedReader(new InputStreamReader(resource.getInputStream())));
        } catch (IOException e) {
            throw new ScriptingException("Failed to load script resource [" + resource + "]", e);
        }
    }
}
