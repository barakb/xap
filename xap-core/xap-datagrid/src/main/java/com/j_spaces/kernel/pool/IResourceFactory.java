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

/*
 * Title:        IResourceFactory.java
 * Description:  Resource Factory for creating resources
 * Company:      GigaSpaces Technologies
 * 
 * @author		 Guy korland
 * @author       Moran Avigdor
 * @version      1.0 01/06/2005
 * @since		 4.1 Build#1173
 */
package com.j_spaces.kernel.pool;


/**
 * IResourceFactory interface for allocating new resources out of the ResourcesPool.
 *
 * Implementing classes should hold an instance of the {@linkplain com.j_spaces.kernel.pool.ResourcePool
 * ResourcePool}
 *
 * @see com.j_spaces.kernel.pool.ResourcePool
 */
public interface IResourceFactory<R extends IResource> {
    /**
     * Allocates a new resource instance.
     *
     * @return a new instance of type {@link IResource}
     */
    R allocate();
}
