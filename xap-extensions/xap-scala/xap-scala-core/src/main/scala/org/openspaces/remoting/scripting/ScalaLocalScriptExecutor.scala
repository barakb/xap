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

import java.util.{Map => JMap}

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.tools.reflect.{ToolBox, ToolBoxError}
import scala.util.Properties

/**
 * Scala local script executor.
 * 
 * @since 9.6
 * @author Dan Kilman
 */
class ScalaLocalScriptExecutor extends AbstractLocalScriptExecutor[java.util.Map[String, Object] => Object] {

  val mirror = universe.runtimeMirror(getClass.getClassLoader)
  val toolbox = ToolBox(mirror).mkToolBox()
  val toolBoxLock = new Object
  
  protected def doCompile(script: Script): JMap[String, Object] => Object = {
    try {
        if (!script.isInstanceOf[ScalaTypedScript]) {
          throw new IllegalArgumentException("script must be a typed scala script")
        }
        val typedScript = script.asInstanceOf[ScalaTypedScript]
        val paramTypes = typedScript.getParameterTypes()
        if (paramTypes eq null) {
          throw new IllegalArgumentException("typed scala script must be configured with static binding types")
        }
        
        val wrappedUserCode = ScalaLocalScriptExecutor.wrapUserCode(typedScript.getScriptAsString(), paramTypes)

        // The toolbox is not thread safe
        toolBoxLock.synchronized {
          val parsedTree = toolbox.parse(wrappedUserCode)
          val compiledScriptHolder = toolbox.compile(parsedTree)
          compiledScriptHolder().asInstanceOf[JMap[String, Object] => Object]
        }
        
    } catch {
      case toolBoxError: ToolBoxError => throw new ScriptCompilationException("Failed compiling scala script", toolBoxError)
      case ex: Exception => throw new ScriptCompilationException("Failed compiling scala script", ex)
    }
  }
  
  def execute(
      script: Script, 
      compiledScript: JMap[String,Object] => Object, 
      parameters: JMap[String,Object]): Object = {
    try {
      compiledScript(parameters)
    } catch {
      case e: Exception => throw new ScriptExecutionException("Failed executing scala script", e)
    }
  }
  
  def close(compiledScript: JMap[String,Object] => Object) = Unit
  def isThreadSafe(): Boolean = false
  
}

object ScalaLocalScriptExecutor {

  val defaultValueMethodName = s"${classOf[ScalaLocalScriptExecutor].getName()}.defaultValue"
  
  def defaultValue[T: ClassTag]: T = {
    val classTag = scala.reflect.classTag[T]
    classTag.runtimeClass.toString match {
      case "void" => ().asInstanceOf[T]
      case "boolean" => false.asInstanceOf[T]
      case "byte" => (0: Byte).asInstanceOf[T]
      case "short" => (0: Short).asInstanceOf[T]
      case "char" => '\u0000'.asInstanceOf[T]
      case "int" => 0.asInstanceOf[T]
      case "long" => 0L.asInstanceOf[T]
      case "float" => 0.0F.asInstanceOf[T]
      case "double" => 0.0.asInstanceOf[T]
      case _ => null.asInstanceOf[T]
    }
  }
  
  private def toScalaTypeName(paramType: Class[_]): String = {
    if (paramType.isArray()) {
      s"Array[${toScalaTypeName(paramType.getComponentType())}]"
    } else {
      paramType.getName() match {
        case "void" => "Unit"
        case "boolean" => "Boolean"
        case "byte" => "Byte"
        case "short" => "Short"
        case "char" => "Char"
        case "int" => "Int"
        case "long" => "Long"
        case "float" => "Float"
        case "double" => "Double"
        case _ => paramType.getName() + createParametersStringIfNeeded(paramType)
      }
    }
  }
  
  private def createParametersStringIfNeeded(paramType: Class[_]): String = {
    if ((paramType.getTypeParameters() ne null) && paramType.getTypeParameters().length > 0) {
      val count = paramType.getTypeParameters().length
      List.fill(count)("_").mkString("[", ",", "]")
    } else {
       ""
    }
  }
  
  private def wrapUserCode(userCode: String, paramTypes: Map[String, Class[_]]): String = {
    s"""
    ${ 
      paramTypes.map({ case (name, paramType) => {
        val scalaTypeName = toScalaTypeName(paramType)
        s"var ${name}: ${scalaTypeName} = ${defaultValueMethodName}[${scalaTypeName}] "
      }}).mkString(Properties.lineSeparator) 
    }
    
    import org.openspaces.scala.core.ScalaGigaSpacesImplicits.ScalaAsyncFutureListener
    import org.openspaces.scala.core.ScalaGigaSpacesImplicits.ScalaEnhancedGigaSpaceWrapper
    import org.openspaces.scala.core.ScalaGigaSpacesImplicits.QueryMacroStringImplicits
    import org.openspaces.scala.core.ScalaGigaSpacesImplicits.QueryMacroDateImplicits
    import org.openspaces.scala.core.MacroDirectives.{select, orderBy, groupBy}
    import org.openspaces.scala.core.GigaSpaceMacroPredicateWrapper
    
    { params: java.util.Map[String, Object] =>

      ${ 
        paramTypes.map({ case(name, paramType) => {
          val scalaTypeName = toScalaTypeName(paramType)
          s"""${name} = params.get(\"${name}\").asInstanceOf[${scalaTypeName}]"""
        }}).mkString(Properties.lineSeparator) 
      }
    
      { 
        ${userCode} 
      }
    }
    """
  }

}
