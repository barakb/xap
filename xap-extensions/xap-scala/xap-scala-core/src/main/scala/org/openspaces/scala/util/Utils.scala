package org.openspaces.scala.util

/**
 * Utilty methods.
 */
object Utils {

  type Closeable = { def close(): Unit }
  
  /**
   * taken from https://github.com/bmc/grizzled-scala/blob/master/src/main/scala/grizzled/io/util.scala
   */
  def withCloseable[C <% Closeable, T](closeable: C)(block: C => T) = {
    try {
      block(closeable)
    } finally {
      closeable.close
    }
  }
  
}