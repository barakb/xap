package org.openspaces.scala.util

import java.io.BufferedReader

import scala.tools.nsc.interpreter._

/**
 * Reader responsible for reading file with initial imports for REPL.
 *
 * @since 10.1
 * @author Bartosz Stalewski
 */
class ImportReader(in: BufferedReader,
                   out: JPrintWriter) extends SimpleReader(in, out, false) {
  override def readOneLine(prompt: String): String = {
    def isImportLine(line: String) = {
      val trimmedLine = line.trim()
      !trimmedLine.isEmpty && !trimmedLine.startsWith("#")
    }

    super.readOneLine(prompt) match {
      case null => null
      case line => if (isImportLine(line)) "import " + line else line
    }
  }
}