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
package org.openspaces.scala.core

import java.io.Serializable
import java.util.concurrent.Future

import com.gigaspaces.async.{AsyncFuture, AsyncFutureListener, AsyncResult}
import com.gigaspaces.client.{ChangeModifiers, ChangeResult, ChangeSet, ClearModifiers, CountModifiers, ReadModifiers, TakeModifiers}
import org.openspaces.core.GigaSpace
import org.openspaces.core.executor.{DistributedTask, Task, TaskGigaSpaceAware}
import org.openspaces.scala.core.makro.GigaSpaceMacros

import scala.language.experimental.macros

/**
 * A set of implicit classes that enhance various OpenSpaces elements.
 * 
 * @since 9.6
 * @author Dan Kilman
 */
object ScalaGigaSpacesImplicits {

  /**
   * Implicit conversion from a function [[com.gigaspaces.async.AsyncResult[T] => Unit]] to [[com.gigaspaces.async.AsyncFutureListener<T>]].
   */
  implicit class ScalaAsyncFutureListener[T](asyncFutureListener: AsyncResult[T] => Unit) 
    extends AsyncFutureListener[T] {
    override def onResult(result: AsyncResult[T]) {
      asyncFutureListener(result)
    }
  }
  
  /**
   * Implicit conversion from [[org.openspaces.core.GigaSpace]] to a wrappen implemention with various methods.
   */
  implicit class ScalaEnhancedGigaSpaceWrapper(val gigaSpace: GigaSpace) {
    
    /**
     * @see [[org.openspaces.core.GigaSpace#execute]]
     */
    def execute[T <: Serializable, R](
      mapper: GigaSpace => T,
      reducer: Seq[AsyncResult[T]] => R): AsyncFuture[R] = {
      gigaSpace.execute(new ScalaDistributedTask(mapper, reducer))
    }
    
    /**
     * @see [[org.openspaces.core.GigaSpace#execute]]
     */
    def execute[T <: Serializable, R](
      mapper: GigaSpace => T,
      reducer: Seq[AsyncResult[T]] => R,
      routing: AnyRef*): AsyncFuture[R] = {
      gigaSpace.execute(new ScalaDistributedTask(mapper, reducer), routing:_*)
    }
    
    /**
     * @see [[org.openspaces.core.GigaSpace#execute]]
     */    
    def execute[T <: Serializable](mapper: GigaSpace => T): AsyncFuture[T] = {
      gigaSpace.execute(new ScalaTask(mapper))
    }
    
    /**
     * @see [[org.openspaces.core.GigaSpace#execute]]
     */
    def execute[T <: Serializable](
        mapper: GigaSpace => T,
        routing: AnyRef): AsyncFuture[T] = {
      gigaSpace.execute(new ScalaTask(mapper), routing)
    }

    /**
     * @see [[org.openspaces.core.GigaSpace#execute]]
     */
    def execute[T <: Serializable](
        mapper: GigaSpace => T,
        routing: AnyRef,
        asyncFutureListener: AsyncResult[T] => Unit): AsyncFuture[T] = {
      gigaSpace.execute(new ScalaTask(mapper), routing, new ScalaAsyncFutureListener(asyncFutureListener))
    }
    
    /**
     * Returns a wrapper around the [[org.openspaces.core.GigaSpace]] instance that provides many predicate based query operations
     * on the space.
     */
    def predicate = new GigaSpaceMacroPredicateWrapper(gigaSpace)
    
  }
  
  /**
   * A set of operators to be used exclusivly within predicate queries on the space.
   * These provide a mechanism for using the matching [[java.lang.String]] operators provided by the SQLQuery API.
   */
  implicit class QueryMacroStringImplicits(value: String) {
    def like    (regex: String): Boolean = ???
    def notLike (regex: String): Boolean = ???
    def rlike   (regex: String): Boolean = ???
  }

  /**
   * A set of operators to be used exclusivly within predicate queries on the space.
   * These provide an enhancements to [[java.util.Date]] which allow comparisons to be made on date instances.
   */
  implicit class QueryMacroDateImplicits(date: java.util.Date) extends Ordered[java.util.Date] {
    def compare(anotherDate: java.util.Date): Int = ???
  }
  
}

/**
 * A set of directive to be used exclusivly within predicate queries on the space.
 * These provided support for projections, order by and group by.
 */
object MacroDirectives {
  def select(properties: Any*): Unit = ???
  def orderBy(properties: Any*): OrderByDirection = ???
  def groupBy(properties: Any*): Unit = ???
  
  trait OrderByDirection {
    val ascending: Unit
    val descending: Unit
  }
}

/**
 * A wrapper around [[org.openspaces.core.GigaSpace]] instances that provides many predicate based query operations
 * on the space.
 * 
 * @see [[org.openspaces.core.GigaSpace]]
 * @since 9.6
 * @author Dan Kilman
 */
class GigaSpaceMacroPredicateWrapper(val gigaSpace: GigaSpace) {
  
  def read[T](predicate: T => Boolean): T = 
    macro GigaSpaceMacros.read_impl[T]
  
  def read[T](predicate: T => Boolean, timeout: Long): T = 
    macro GigaSpaceMacros.readWithTimeout_impl[T]
  
  def read[T](predicate: T => Boolean, timeout: Long, modifiers: ReadModifiers): T = 
    macro GigaSpaceMacros.readWithTimeoutAndModifiers_impl[T]

  def readIfExists[T](predicate: T => Boolean): T = 
    macro GigaSpaceMacros.readIfExists_impl[T]
  
  def readIfExists[T](predicate: T => Boolean, timeout: Long): T = 
    macro GigaSpaceMacros.readIfExistsWithTimeout_impl[T]
  
  def readIfExists[T](predicate: T => Boolean, timeout: Long, modifiers: ReadModifiers): T = 
    macro GigaSpaceMacros.readIfExistsWithTimeoutAndModifiers_impl[T]

  def take[T](predicate: T => Boolean): T = 
    macro GigaSpaceMacros.take_impl[T]
  
  def take[T](predicate: T => Boolean, timeout: Long): T = 
    macro GigaSpaceMacros.takeWithTimeout_impl[T]
  
  def take[T](predicate: T => Boolean, timeout: Long, modifiers: TakeModifiers): T = 
    macro GigaSpaceMacros.takeWithTimeoutAndModifiers_impl[T]

  def takeIfExists[T](predicate: T => Boolean): T = 
    macro GigaSpaceMacros.takeIfExists_impl[T]
  
  def takeIfExists[T](predicate: T => Boolean, timeout: Long): T = 
    macro GigaSpaceMacros.takeIfExistsWithTimeout_impl[T]
  
  def takeIfExists[T](predicate: T => Boolean, timeout: Long, modifiers: TakeModifiers): T = 
    macro GigaSpaceMacros.takeIfExistsWithTimeoutAndModifiers_impl[T]
  
  def count[T](predicate: T => Boolean): Int =
    macro GigaSpaceMacros.count_impl[T]
  
  def count[T](predicate: T => Boolean, modifiers: CountModifiers): Int =
    macro GigaSpaceMacros.countWithModifiers_impl[T]

  def clear[T](predicate: T => Boolean): Unit =
    macro GigaSpaceMacros.clear_impl[T]
  
  def clear[T](predicate: T => Boolean, modifiers: ClearModifiers): Int =
    macro GigaSpaceMacros.clearWithModifiers_impl[T]
  
  def readMultiple[T](predicate: T => Boolean): Array[T] =
    macro GigaSpaceMacros.readMultiple_impl[T]
  
  def readMultiple[T](predicate: T => Boolean, maxEntries: Int): Array[T] =
    macro GigaSpaceMacros.readMultipleWithMaxEntries_impl[T]
  
  def readMultiple[T](predicate: T => Boolean, maxEntries: Int, modifiers: ReadModifiers): Array[T] =
    macro GigaSpaceMacros.readMultipleWithMaxEntriesAndModifiers_impl[T]

  def takeMultiple[T](predicate: T => Boolean): Array[T] =
    macro GigaSpaceMacros.takeMultiple_impl[T]
  
  def takeMultiple[T](predicate: T => Boolean, maxEntries: Int): Array[T] =
    macro GigaSpaceMacros.takeMultipleWithMaxEntries_impl[T]
  
  def takeMultiple[T](predicate: T => Boolean, maxEntries: Int, modifiers: TakeModifiers): Array[T] =
    macro GigaSpaceMacros.takeMultipleWithMaxEntriesAndModifiers_impl[T]
  
  def change[T](predicate: T => Boolean, changeSet: ChangeSet): ChangeResult[T] = 
    macro GigaSpaceMacros.change_impl[T]
  
  def change[T](predicate: T => Boolean, changeSet: ChangeSet, timeout: Long): ChangeResult[T] = 
    macro GigaSpaceMacros.changeWithTimeout_impl[T]

  def change[T](predicate: T => Boolean, changeSet: ChangeSet, modifiers: ChangeModifiers): ChangeResult[T] = 
    macro GigaSpaceMacros.changeWithModifiers_impl[T]
  
  def change[T](predicate: T => Boolean, changeSet: ChangeSet, modifiers: ChangeModifiers, timeout: Long): ChangeResult[T] = 
    macro GigaSpaceMacros.changeWithModifiersAndTimeout_impl[T]
  
  def asyncRead[T](predicate: T => Boolean): AsyncFuture[T] =
    macro GigaSpaceMacros.asyncRead_impl[T]
  
  def asyncRead[T](predicate: T => Boolean, timeout: Long): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncReadWithTimeout_impl[T]
  
  def asyncRead[T](predicate: T => Boolean, timeout: Long, modifiers: ReadModifiers): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncReadWithTimeoutAndModifiers_impl[T]

  def asyncRead[T](predicate: T => Boolean, listener: AsyncFutureListener[T]): AsyncFuture[T] =
    macro GigaSpaceMacros.asyncReadWithListener_impl[T]
  
  def asyncRead[T](predicate: T => Boolean, timeout: Long, listener: AsyncFutureListener[T]): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncReadWithTimeoutAndListener_impl[T]
  
  def asyncRead[T](predicate: T => Boolean, timeout: Long, modifiers: ReadModifiers, listener: AsyncFutureListener[T]): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncReadWithTimeoutAndModifiersAndListener_impl[T]

  def asyncTake[T](predicate: T => Boolean): AsyncFuture[T] =
    macro GigaSpaceMacros.asyncTake_impl[T]
  
  def asyncTake[T](predicate: T => Boolean, timeout: Long): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncTakeWithTimeout_impl[T]
  
  def asyncTake[T](predicate: T => Boolean, timeout: Long, modifiers: TakeModifiers): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncTakeWithTimeoutAndModifiers_impl[T]

  def asyncTake[T](predicate: T => Boolean, listener: AsyncFutureListener[T]): AsyncFuture[T] =
    macro GigaSpaceMacros.asyncTakeWithListener_impl[T]
  
  def asyncTake[T](predicate: T => Boolean, timeout: Long, listener: AsyncFutureListener[T]): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncTakeWithTimeoutAndListener_impl[T]
  
  def asyncTake[T](predicate: T => Boolean, timeout: Long, modifiers: TakeModifiers, listener: AsyncFutureListener[T]): AsyncFuture[T] = 
    macro GigaSpaceMacros.asyncTakeWithTimeoutAndModifiersAndListener_impl[T]

  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChange_impl[T]
  
  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet, timeout: Long): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChangeWithTimeout_impl[T]

  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet, modifiers: ChangeModifiers): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChangeWithModifiers_impl[T]
  
  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet, modifiers: ChangeModifiers, timeout: Long): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChangeWithModifiersAndTimeout_impl[T]

  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet, listener: AsyncFutureListener[ChangeResult[T]]): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChangeWithListener_impl[T]
  
  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet, timeout: Long, listener: AsyncFutureListener[ChangeResult[T]]): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChangeWithTimeoutAndListener_impl[T]

  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet, modifiers: ChangeModifiers, listener: AsyncFutureListener[ChangeResult[T]]): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChangeWithModifiersAndListener_impl[T]
  
  def asyncChange[T](predicate: T => Boolean, changeSet: ChangeSet, modifiers: ChangeModifiers, timeout: Long, listener: AsyncFutureListener[ChangeResult[T]]): Future[ChangeResult[T]] = 
    macro GigaSpaceMacros.asyncChangeWithModifiersAndTimeoutAndListener_impl[T]
  
}

protected class ScalaTask[T <: Serializable](
    mapper: GigaSpace => T)
  extends Task[T]
  with TaskGigaSpaceAware {
  
  var colocatedGigaSpace: GigaSpace = _
  
  override def setGigaSpace(colocatedGigaSpace: GigaSpace) {
    this.colocatedGigaSpace = colocatedGigaSpace
  }
  
  override def execute(): T = mapper(colocatedGigaSpace)
}

protected class ScalaDistributedTask[T <: Serializable, R](
    mapper: GigaSpace => T,
    reducer: Seq[AsyncResult[T]] => R)
  extends ScalaTask[T](mapper)
  with DistributedTask[T, R] {
  
  override def reduce(results: java.util.List[AsyncResult[T]]): R = 
    reducer(scala.collection.JavaConversions.asScalaBuffer(results))
  
}

