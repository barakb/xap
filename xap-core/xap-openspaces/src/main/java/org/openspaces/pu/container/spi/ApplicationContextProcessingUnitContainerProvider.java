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


package org.openspaces.pu.container.spi;

import org.openspaces.pu.container.ProcessingUnitContainerProvider;
import org.springframework.core.io.Resource;

import java.io.IOException;

/**
 * @author kimchy
 */
public abstract class ApplicationContextProcessingUnitContainerProvider extends ProcessingUnitContainerProvider {

    public static final String DEFAULT_PU_CONTEXT_LOCATION = "classpath*:/META-INF/spring/pu.xml";

    protected static final String DEFAULT_FS_PU_CONTEXT_LOCATION = "META-INF/spring/pu.xml";

    public abstract void addConfigLocation(String configLocation) throws IOException;

    public abstract void addConfigLocation(Resource resource) throws IOException;
}
