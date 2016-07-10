/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Various type aliases grouped together for convenience.
 * 
 * @since 9.6
 * @author Dan Kilman
 */
package org.openspaces.scala.core.aliases {
  
 /**
  * Various openspaces type aliases grouped together for convenience.
  * 
  * @since 9.6
  * @author Dan Kilman
  */
  package object misc {
    
    // Silly short names
    type G     = org.openspaces.core.GigaSpace
    type S     = com.j_spaces.core.IJSpace
    type Q[T]  = com.gigaspaces.query.ISpaceQuery[T]
    type D     = com.gigaspaces.document.SpaceDocument
  }
  
 /**
  * Same annotations found under package: [[com.gigaspaces.annotation.pojo]].
  * The annotations in this package have been enhanced with the beanGetter annotation
  * To save some boilerplate at usage site.
  * 
  * @since 9.6
  * @author Dan Kilman
  */
  package object annotation {
    
    import com.gigaspaces.annotation.pojo

    import scala.annotation.meta.beanGetter
    
    type SpaceClass                = pojo.SpaceClass
    type SpaceClassConstructor     = pojo.SpaceClassConstructor
    
    // Enhance space annotations with @beanGetter property
    type SpaceDynamicProperties    = pojo.SpaceDynamicProperties @beanGetter
    type SpaceExclude              = pojo.SpaceExclude @beanGetter
    type SpaceFifoGroupingIndex    = pojo.SpaceFifoGroupingIndex @beanGetter
    type SpaceFifoGroupingProperty = pojo.SpaceFifoGroupingProperty @beanGetter
    type SpaceId                   = pojo.SpaceId @beanGetter
    type SpaceIndex                = pojo.SpaceIndex @beanGetter
    type SpaceIndexes              = pojo.SpaceIndexes @beanGetter
    type SpaceLeaseExpiration      = pojo.SpaceLeaseExpiration @beanGetter
    type SpacePersist              = pojo.SpacePersist @beanGetter
    type SpaceProperty             = pojo.SpaceProperty @beanGetter
    type SpaceRouting              = pojo.SpaceRouting @beanGetter
    type SpaceStorageType          = pojo.SpaceStorageType @beanGetter
    type SpaceVersion              = pojo.SpaceVersion @beanGetter  
    
  }
  
}
