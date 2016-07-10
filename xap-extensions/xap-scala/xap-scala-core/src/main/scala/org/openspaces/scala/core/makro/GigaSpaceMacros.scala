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
package org.openspaces.scala.core.makro

import java.util.concurrent.Future

import com.gigaspaces.async.{AsyncFuture, AsyncFutureListener}
import com.gigaspaces.client.{ChangeModifiers, ChangeResult, ChangeSet, ClearModifiers, CountModifiers, ReadModifiers, TakeModifiers}
import org.openspaces.scala.core.ScalaGigaSpacesImplicits

import scala.reflect.macros.blackbox.Context

/**
 * Thin wrapper for macro based operation. 
 * The heavy lifiting is delegated to [[org.openspaces.scala.core.makro.GigaSpaceMacroHelper]].
 * 
 * @since 9.6
 * @author Dan Kilman
 */
object GigaSpaceMacros {

  type WrapperType = ScalaGigaSpacesImplicits.ScalaEnhancedGigaSpaceWrapper
  type TypedContext = Context { type PrefixType = WrapperType }
  
  def read_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val ifExists = false
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def readWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout) 
      override val ifExists = false
      override val take = false
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def readWithTimeoutAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[ReadModifiers]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val ifExists = false
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }

  def readIfExists_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1 
      override val ifExists = true
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def readIfExistsWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout) 
      override val ifExists = true
      override val take = false
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def readIfExistsWithTimeoutAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[ReadModifiers]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val ifExists = true
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }

  def take_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val ifExists = false
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def takeWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout) 
      override val ifExists = false
      override val take = true
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def takeWithTimeoutAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[TakeModifiers]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val ifExists = false
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }

  def takeIfExists_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1 
      override val ifExists = true
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def takeIfExistsWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout) 
      override val ifExists = true
      override val take = true
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def takeIfExistsWithTimeoutAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[TakeModifiers]): c1.Expr[T] = {
    val helper = new ReadTakeMacroHelper { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val ifExists = true
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  } 
 
  def count_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[Int] = {
    val helper = new CountClearMacroHelper { 
      override val c: c1.type = c1
      override val clear = false
    }
    c1.Expr(helper.generate(predicate))
  }

  def countWithModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     modifiers: c1.Expr[CountModifiers]): c1.Expr[Int] = {
    val helper = new CountClearMacroHelper { 
      override val c: c1.type = c1
      override val modifiersOption = Some(modifiers)
      override val clear = false
    }
    c1.Expr(helper.generate(predicate))
  }

  def clear_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[Unit] = {
    val helper = new CountClearMacroHelper { 
      override val c: c1.type = c1
      override val clear = true
    }
    c1.Expr(helper.generate(predicate))
  }

  def clearWithModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     modifiers: c1.Expr[ClearModifiers]): c1.Expr[Int] = {
    val helper = new CountClearMacroHelper { 
      override val c: c1.type = c1
      override val modifiersOption = Some(modifiers)
      override val clear = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def readMultiple_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[Array[T]] = {
    val helper = new ReadTakeMultipleMacroHelper { 
      override val c: c1.type = c1
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def readMultipleWithMaxEntries_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     maxEntries: c1.Expr[Int]): c1.Expr[Array[T]] = {
    val helper = new ReadTakeMultipleMacroHelper { 
      override val c: c1.type = c1
      override val maxEntriesOption = Some(maxEntries)
      override val take = false
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def readMultipleWithMaxEntriesAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     maxEntries: c1.Expr[Int],
     modifiers: c1.Expr[ReadModifiers]): c1.Expr[Array[T]] = {
    val helper = new ReadTakeMultipleMacroHelper { 
      override val c: c1.type = c1
      override val maxEntriesOption = Some(maxEntries)
      override val modifiersOption = Some(modifiers)
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def takeMultiple_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[Array[T]] = {
    val helper = new ReadTakeMultipleMacroHelper { 
      override val c: c1.type = c1
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def takeMultipleWithMaxEntries_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     maxEntries: c1.Expr[Int]): c1.Expr[Array[T]] = {
    val helper = new ReadTakeMultipleMacroHelper { 
      override val c: c1.type = c1
      override val maxEntriesOption = Some(maxEntries)
      override val take = true
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def takeMultipleWithMaxEntriesAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     maxEntries: c1.Expr[Int],
     modifiers: c1.Expr[TakeModifiers]): c1.Expr[Array[T]] = {
    val helper = new ReadTakeMultipleMacroHelper { 
      override val c: c1.type = c1
      override val maxEntriesOption = Some(maxEntries)
      override val modifiersOption = Some(modifiers)
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def change_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet]): c1.Expr[ChangeResult[T]] = {
    val helper = new ChangeMacroHelper { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def changeWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     timeout: c1.Expr[Long]): c1.Expr[ChangeResult[T]] = {
    val helper = new ChangeMacroHelper { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val timeoutOption = Some(timeout)
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def changeWithModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     modifiers: c1.Expr[ChangeModifiers]): c1.Expr[ChangeResult[T]] = {
    val helper = new ChangeMacroHelper { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val modifiersOption = Some(modifiers)
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def changeWithModifiersAndTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     modifiers: c1.Expr[ChangeModifiers],
     timeout: c1.Expr[Long]): c1.Expr[ChangeResult[T]] = {
    val helper = new ChangeMacroHelper { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
    }
    c1.Expr(helper.generate(predicate))
  }
 
  def asyncRead_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncReadWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout) 
      override val take = false
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncReadWithTimeoutAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[ReadModifiers]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncReadWithListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     listener: c1.Expr[AsyncFutureListener[T]]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val futureListenerOption = Some(listener)
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncReadWithTimeoutAndListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     listener: c1.Expr[AsyncFutureListener[T]]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val futureListenerOption = Some(listener)
      override val take = false
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncReadWithTimeoutAndModifiersAndListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[ReadModifiers],
     listener: c1.Expr[AsyncFutureListener[T]]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val futureListenerOption = Some(listener)
      override val take = false
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncTake_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncTakeWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout) 
      override val take = true
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncTakeWithTimeoutAndModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[TakeModifiers]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncTakeWithListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     listener: c1.Expr[AsyncFutureListener[T]]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val futureListenerOption = Some(listener)
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncTakeWithTimeoutAndListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     listener: c1.Expr[AsyncFutureListener[T]]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val futureListenerOption = Some(listener)
      override val take = true
    } 
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncTakeWithTimeoutAndModifiersAndListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     timeout: c1.Expr[Long],
     modifiers: c1.Expr[TakeModifiers],
     listener: c1.Expr[AsyncFutureListener[T]]): c1.Expr[AsyncFuture[T]] = {
    val helper = new AsyncReadTakeMacroHelper[T] { 
      override val c: c1.type = c1
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val futureListenerOption = Some(listener)
      override val take = true
    }
    c1.Expr(helper.generate(predicate))
  }
 
  def asyncChange_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncChangeWithTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     timeout: c1.Expr[Long]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val timeoutOption = Some(timeout)
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncChangeWithModifiers_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     modifiers: c1.Expr[ChangeModifiers]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val modifiersOption = Some(modifiers)
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncChangeWithModifiersAndTimeout_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     modifiers: c1.Expr[ChangeModifiers],
     timeout: c1.Expr[Long]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
    }
    c1.Expr(helper.generate(predicate))
  }

  def asyncChangeWithListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     listener: c1.Expr[AsyncFutureListener[ChangeResult[T]]]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val futureListenerOption = Some(listener)
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncChangeWithTimeoutAndListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     timeout: c1.Expr[Long],
     listener: c1.Expr[AsyncFutureListener[ChangeResult[T]]]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val timeoutOption = Some(timeout)
      override val futureListenerOption = Some(listener)
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncChangeWithModifiersAndListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     modifiers: c1.Expr[ChangeModifiers],
     listener: c1.Expr[AsyncFutureListener[ChangeResult[T]]]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val modifiersOption = Some(modifiers)
      override val futureListenerOption = Some(listener)
    }
    c1.Expr(helper.generate(predicate))
  }
  
  def asyncChangeWithModifiersAndTimeoutAndListener_impl[T](c1: TypedContext)
    (predicate: c1.Expr[T => Boolean],
     changeSet: c1.Expr[ChangeSet],
     modifiers: c1.Expr[ChangeModifiers],
     timeout: c1.Expr[Long],
     listener: c1.Expr[AsyncFutureListener[ChangeResult[T]]]): c1.Expr[Future[ChangeResult[T]]] = {
    val helper = new AsyncChangeMacroHelper[T] { 
      override val c: c1.type = c1
      override val changeSetExpr = changeSet
      override val timeoutOption = Some(timeout)
      override val modifiersOption = Some(modifiers)
      override val futureListenerOption = Some(listener)
    }
    c1.Expr(helper.generate(predicate))
  }
  
}

