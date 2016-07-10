package org.openspaces.remoting.scripting

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.junit.{Assert, Test}

class ScalaTypedScriptTest {

  @Test
  def serializationTest() {

    val staticScript = new ScalaTypedStaticScript("staticScript", "scala", "1")
      .cache(false)
      .parameter("dan", "k")
      .parameterType("dan", classOf[String])
      
    val bytes = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bytes)
    staticScript.writeExternal(out)
    out.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))
    val deserializedStaticScript = new ScalaTypedStaticScript()
    deserializedStaticScript.readExternal(in)

    Assert.assertEquals(staticScript.getName(), deserializedStaticScript.getName())
    Assert.assertEquals(staticScript.getType(), deserializedStaticScript.getType())
    Assert.assertEquals(staticScript.getScriptAsString(), deserializedStaticScript.getScriptAsString())
    Assert.assertEquals(staticScript.shouldCache(), deserializedStaticScript.shouldCache())
    Assert.assertEquals(staticScript.getParameters(), deserializedStaticScript.getParameters())
    Assert.assertEquals(staticScript.getParameterTypes, deserializedStaticScript.getParameterTypes)
    
  }
  
}