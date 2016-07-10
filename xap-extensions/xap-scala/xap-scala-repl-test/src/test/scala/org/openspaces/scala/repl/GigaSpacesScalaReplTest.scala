package org.openspaces.scala.repl

import org.junit.{Assert, Test}

import scala.tools.nsc.interpreter.JPrintWriter
import scala.tools.nsc.util._
import scala.tools.nsc.{GenericRunnerSettings, Properties, Settings}

class GigaSpacesScalaReplTest {

  val prompt = "\nxap>"

  @Test
  def emptyRunTest() = {
    val output = replOutputFor(":quit")
    Assert.assertEquals("", output)
  }

  @Test
  def runWithNewInitScalaTest() = {
    System.setProperty("org.os.scala.repl.newinitstyle", "true")
    try {
      val output = replOutputFor(":quit")
      Assert.assertEquals("", output)
    } finally {
      System.clearProperty("org.os.scala.repl.newinitstyle")
    }
  }

  @Test
  def replImportsTest() = {
    val output = replOutputFor("classOf[SpaceDocument]\nclassOf[GigaSpace]")
    val expectedOutput = """res1: Class[com.gigaspaces.document.SpaceDocument] = class com.gigaspaces.document.SpaceDocument
      |res2: Class[org.openspaces.core.GigaSpace] = interface org.openspaces.core.GigaSpace""".stripMargin
    Assert.assertEquals(expectedOutput, output)
  }

  private def replOutputFor(input: String) = {
    val settings = new GenericRunnerSettings(Console.println)
    settings.usejavacp.value = true

    val output = run(input, settings)
    cleanOutput(output)
  }

  private def cleanOutput(output: String): String = {
    output.split(prompt).drop(1).dropRight(1).map(_.trim).mkString("\n")
  }

  private def run(code: String, sets: Settings = new Settings): String = {
    // Used during tests. This is implemented in the same way as in ILoop.
    import java.io.{BufferedReader, OutputStreamWriter, StringReader}

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input    = new BufferedReader(new StringReader(code))
        val output   = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl = new GigaSpacesScalaReplLoop(Some(input), output) {
          override protected def getBaseDirectory =
            Properties.propOrElse("user.dir", ".") + "/../xap-scala-core/tools/scala/conf"
        }

        repl process sets
      }
    }
  }
}
