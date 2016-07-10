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
package org.openspaces.remoting.scripting

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.{Map => JMap}

import com.gigaspaces.internal.io.IOUtils
import com.gigaspaces.internal.utils.ObjectUtils
import com.j_spaces.kernel.ClassLoaderHelper
import org.openspaces.remoting.RemoteResultReducer

import scala.beans.BeanProperty
import scala.collection.JavaConversions.mapAsScalaMap

/**
 * A trait that is included in all scala based typed scripts.
 * 
 * @since 9.6
 * @author Dan Kilman
 */
trait ScalaTypedScript extends TypedScript with Externalizable {
  
  @BeanProperty val parameterTypes: JMap[String, Class[_]] = new java.util.HashMap[String, Class[_]]()

  def parameterType(name: String, staticType: Class[_]): this.type = { parameterTypes.put(name, staticType); this }
  
  abstract override def writeExternal(out: ObjectOutput) {
    super.writeExternal(out)
    writeParameterTypesMap(out)
  }
  abstract override def readExternal(in: ObjectInput) {
    super.readExternal(in)
    readParameterTypesMap(in)
  }
  protected def writeParameterTypesMap(out: ObjectOutput) {
    if (parameterTypes eq null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeShort(parameterTypes.size)
      parameterTypes.foreach { case(paramName, paramType) =>
        IOUtils.writeString(out, paramName)
        IOUtils.writeString(out, paramType.getName())
      }
    }
  }
  protected def readParameterTypesMap(in: ObjectInput) {
    if (in.readBoolean()) {
      val size = in.readShort()
      for (i <- 0 until size) {
        val paramName = IOUtils.readString(in)
        val paramTypeName = IOUtils.readString(in)
        val paramType = 
          if (ObjectUtils.isPrimitive(paramTypeName)) 
            ObjectUtils.getPrimitive(paramTypeName)
          else
            ClassLoaderHelper.loadClass(paramTypeName, false /* localOnly */);
        parameterTypes.put(paramName, paramType)
      }
    }
  }
  
}

/**
 * An extenstion of [[org.openspaces.remoting.scripting.StaticScript]] with the ability to define
 * types for paramters.
 * 
 * @since 9.6
 * @author Dan Kilman
 */
class ScalaTypedStaticScript(name: String, scriptType: String, code: String) 
  extends StaticScript(name, scriptType, code)
  with ScalaTypedScript {

  def this() = this(null, null, null)
  
  override def parameter(name: String, value: Any) = { super.parameter(name, value); this }
  override def name(name: String) = { super.name(name); this }
  override def script(script: String) = { super.script(script); this }
  override def `type`(`type`: String) = { super.`type`(`type`); this }
  override def cache(shouldCache: Boolean) = { super.cache(shouldCache); this }
  override def routing(routing: Any) = { super.routing(routing); this }
  override def broadcast[T, Y](reducer: RemoteResultReducer[T, Y]) = { super.broadcast(reducer); this }
  
  def parameter(name: String, value: Any, staticType: Class[_]) = {
    super.parameter(name, value)
    parameterType(name, staticType)
  }
}

/**
 * An extenstion of [[org.openspaces.remoting.scripting.StaticResourceScript]] with the ability to define
 * types for paramters.
 * 
 * @since 9.6
 * @author Dan Kilman
 */
class ScalaTypedStaticResourceScript(name: String, scriptType: String, resourceLocation: String) 
  extends StaticResourceScript(name, scriptType, resourceLocation)
  with ScalaTypedScript {
  
  def this() = this(null, null, null)
  
  override def parameter(name: String, value: Any) = { super.parameter(name, value); this }
  override def name(name: String) = { super.name(name); this }
  override def script(script: String) = { super.script(script); this }
  override def `type`(`type`: String) = { super.`type`(`type`); this }
  override def cache(shouldCache: Boolean) = { super.cache(shouldCache); this }
  override def routing(routing: Any) = { super.routing(routing); this }
  override def broadcast[T, Y](reducer: RemoteResultReducer[T, Y]) = { super.broadcast(reducer); this }
  
  def parameter(name: String, value: Any, staticType: Class[_]) = {
    super.parameter(name, value)
    parameterType(name, staticType)
  }
}

/**
 * An extenstion of [[org.openspaces.remoting.scripting.ResourceLazyLoadingScript]] with the ability to define
 * types for paramters.
 * 
 * @since 9.6
 * @author Dan Kilman
 */
class ScalaTypedResourceLazyLoadingScript(name: String, scriptType: String, resourceLocation: String) 
  extends ResourceLazyLoadingScript(name, scriptType, resourceLocation)
  with ScalaTypedScript {
  
  def this() = this(null, null, null)
  
  override def parameter(name: String, value: Any) = { super.parameter(name, value); this }
  override def name(name: String) = { super.name(name); this }
  override def script(script: String) = { super.script(script); this }
  override def `type`(`type`: String) = { super.`type`(`type`); this }
  override def cache(shouldCache: Boolean) = { super.cache(shouldCache); this }
  override def routing(routing: Any) = { super.routing(routing); this }
  override def broadcast[T, Y](reducer: RemoteResultReducer[T, Y]) = { super.broadcast(reducer); this }
  
  def parameter(name: String, value: Any, staticType: Class[_]) = { 
    super.parameter(name, value)
    parameterType(name, staticType)
  }
}

