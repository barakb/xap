package org.openspaces.scala.core.makro

import java.util.Date

import com.gigaspaces.async.AsyncFutureListener
import com.gigaspaces.client.{ChangeModifiers, ChangeResult, ChangeSet, ClearModifiers, CountModifiers, ReadModifiers, TakeModifiers}
import com.j_spaces.core.client.SQLQuery
import org.junit.Test
import org.mockito.Mockito._
import org.openspaces.core.GigaSpace
import org.openspaces.scala.core.ScalaGigaSpacesImplicits._

case class Person(name: String = null, 
                  age: Int = 0, 
                  son: Person = null, 
                  birthday: Date = null, 
                  originalQuery: SQLQuery[Person] = null)

class GigaSpaceMacrosApiTest {

  val gigaSpace: GigaSpace = mock(classOf[GigaSpace])
  val sqlQuery = new SQLQuery(classOf[Person], "name = ?", "john")
  val changeSet = new ChangeSet().set("name", "jane")
  val timeout = 2l
  val maxEntries = 3
  val readModifiers = ReadModifiers.DIRTY_READ
  val takeModifiers = TakeModifiers.EVICT_ONLY
  val changeModifiers = ChangeModifiers.ONE_WAY
  val countModifiers = CountModifiers.READ_COMMITTED
  val clearModifiers = ClearModifiers.MEMORY_ONLY_SEARCH
  val readTakeListener = mock(classOf[AsyncFutureListener[Person]])
  val changeListener = mock(classOf[AsyncFutureListener[ChangeResult[Person]]])
  
  @Test
  def testApi() {
    doMacroInvocations()
    verifyApiInvocations()
  }
  
  private def doMacroInvocations() {
    val pGigaSpace = gigaSpace.predicate
    pGigaSpace.read        ({ p: Person => p.name == "john" })
    pGigaSpace.read        ({ p: Person => p.name == "john" }, timeout)
    pGigaSpace.read        ({ p: Person => p.name == "john" }, timeout, readModifiers)
    pGigaSpace.readIfExists({ p: Person => p.name == "john" })
    pGigaSpace.readIfExists({ p: Person => p.name == "john" }, timeout)
    pGigaSpace.readIfExists({ p: Person => p.name == "john" }, timeout, readModifiers)
    pGigaSpace.take        ({ p: Person => p.name == "john" })
    pGigaSpace.take        ({ p: Person => p.name == "john" }, timeout)
    pGigaSpace.take        ({ p: Person => p.name == "john" }, timeout, takeModifiers)
    pGigaSpace.takeIfExists({ p: Person => p.name == "john" })
    pGigaSpace.takeIfExists({ p: Person => p.name == "john" }, timeout)
    pGigaSpace.takeIfExists({ p: Person => p.name == "john" }, timeout, takeModifiers)
    pGigaSpace.count       ({ p: Person => p.name == "john" })
    pGigaSpace.count       ({ p: Person => p.name == "john" }, countModifiers)
    pGigaSpace.clear       ({ p: Person => p.name == "john" })
    pGigaSpace.clear       ({ p: Person => p.name == "john" }, clearModifiers)
    pGigaSpace.readMultiple({ p: Person => p.name == "john" })
    pGigaSpace.readMultiple({ p: Person => p.name == "john" }, maxEntries)
    pGigaSpace.readMultiple({ p: Person => p.name == "john" }, maxEntries, readModifiers)
    pGigaSpace.takeMultiple({ p: Person => p.name == "john" })
    pGigaSpace.takeMultiple({ p: Person => p.name == "john" }, maxEntries)
    pGigaSpace.takeMultiple({ p: Person => p.name == "john" }, maxEntries, takeModifiers)
    pGigaSpace.change      ({ p: Person => p.name == "john" }, changeSet)
    pGigaSpace.change      ({ p: Person => p.name == "john" }, changeSet, timeout)
    pGigaSpace.change      ({ p: Person => p.name == "john" }, changeSet, changeModifiers)
    pGigaSpace.change      ({ p: Person => p.name == "john" }, changeSet, changeModifiers, timeout)
    pGigaSpace.asyncRead   ({ p: Person => p.name == "john" })
    pGigaSpace.asyncRead   ({ p: Person => p.name == "john" }, timeout)
    pGigaSpace.asyncRead   ({ p: Person => p.name == "john" }, timeout, readModifiers)
    pGigaSpace.asyncRead   ({ p: Person => p.name == "john" }, readTakeListener)
    pGigaSpace.asyncRead   ({ p: Person => p.name == "john" }, timeout, readTakeListener)
    pGigaSpace.asyncRead   ({ p: Person => p.name == "john" }, timeout, readModifiers, readTakeListener)
    pGigaSpace.asyncTake   ({ p: Person => p.name == "john" })
    pGigaSpace.asyncTake   ({ p: Person => p.name == "john" }, timeout)
    pGigaSpace.asyncTake   ({ p: Person => p.name == "john" }, timeout, takeModifiers)
    pGigaSpace.asyncTake   ({ p: Person => p.name == "john" }, readTakeListener)
    pGigaSpace.asyncTake   ({ p: Person => p.name == "john" }, timeout, readTakeListener)
    pGigaSpace.asyncTake   ({ p: Person => p.name == "john" }, timeout, takeModifiers, readTakeListener)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet, timeout)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet, changeModifiers)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet, changeModifiers, timeout)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet, changeListener)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet, timeout, changeListener)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet, changeModifiers, changeListener)
    pGigaSpace.asyncChange ({ p: Person => p.name == "john" }, changeSet, changeModifiers, timeout, changeListener)
  }
  
  private def verifyApiInvocations() {
    verify(gigaSpace).read        (sqlQuery)
    verify(gigaSpace).read        (sqlQuery, timeout)
    verify(gigaSpace).read        (sqlQuery, timeout, readModifiers)
    verify(gigaSpace).readIfExists(sqlQuery)
    verify(gigaSpace).readIfExists(sqlQuery, timeout)
    verify(gigaSpace).readIfExists(sqlQuery, timeout, readModifiers)
    verify(gigaSpace).take        (sqlQuery)
    verify(gigaSpace).take        (sqlQuery, timeout)
    verify(gigaSpace).take        (sqlQuery, timeout, takeModifiers)
    verify(gigaSpace).takeIfExists(sqlQuery)
    verify(gigaSpace).takeIfExists(sqlQuery, timeout)
    verify(gigaSpace).takeIfExists(sqlQuery, timeout, takeModifiers)
    verify(gigaSpace).count       (sqlQuery)
    verify(gigaSpace).count       (sqlQuery, countModifiers)
    verify(gigaSpace).clear       (sqlQuery)
    verify(gigaSpace).clear       (sqlQuery, clearModifiers)  
    verify(gigaSpace).readMultiple(sqlQuery)
    verify(gigaSpace).readMultiple(sqlQuery, maxEntries)
    verify(gigaSpace).readMultiple(sqlQuery, maxEntries, readModifiers)
    verify(gigaSpace).takeMultiple(sqlQuery)
    verify(gigaSpace).takeMultiple(sqlQuery, maxEntries)
    verify(gigaSpace).takeMultiple(sqlQuery, maxEntries, takeModifiers)
    verify(gigaSpace).change      (sqlQuery, changeSet)
    verify(gigaSpace).change      (sqlQuery, changeSet, timeout)
    verify(gigaSpace).change      (sqlQuery, changeSet, changeModifiers)
    verify(gigaSpace).change      (sqlQuery, changeSet, changeModifiers, timeout)
    verify(gigaSpace).asyncRead   (sqlQuery)
    verify(gigaSpace).asyncRead   (sqlQuery, timeout)
    verify(gigaSpace).asyncRead   (sqlQuery, timeout, readModifiers)
    verify(gigaSpace).asyncRead   (sqlQuery, readTakeListener)
    verify(gigaSpace).asyncRead   (sqlQuery, timeout, readTakeListener)
    verify(gigaSpace).asyncRead   (sqlQuery, timeout, readModifiers, readTakeListener)
    verify(gigaSpace).asyncTake   (sqlQuery)
    verify(gigaSpace).asyncTake   (sqlQuery, timeout)
    verify(gigaSpace).asyncTake   (sqlQuery, timeout, takeModifiers)
    verify(gigaSpace).asyncTake   (sqlQuery, readTakeListener)
    verify(gigaSpace).asyncTake   (sqlQuery, timeout, readTakeListener)
    verify(gigaSpace).asyncTake   (sqlQuery, timeout, takeModifiers, readTakeListener)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet, timeout)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet, changeModifiers)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet, changeModifiers, timeout)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet, changeListener)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet, timeout, changeListener)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet, changeModifiers, changeListener)
    verify(gigaSpace).asyncChange (sqlQuery, changeSet, changeModifiers, timeout, changeListener)
  }
}
