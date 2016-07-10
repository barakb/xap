package org.openspaces.scala.core.makro

import java.io.ByteArrayInputStream
import java.util.Date

import com.gigaspaces.query.{ISpaceQuery, QueryResultType}
import com.j_spaces.core.client.SQLQuery
import com.j_spaces.jdbc.parser.grammar.SqlParser
import org.junit._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.openspaces.core.space.UrlSpaceConfigurer
import org.openspaces.core.{GigaSpace, GigaSpaceConfigurer}
import org.openspaces.scala.core.MacroDirectives._
import org.openspaces.scala.core.ScalaGigaSpacesImplicits._


/*
  New space needs to be created to properly initiate QueryProcessorConfiguration.
  Otherwise mocked space will not initiate configuration and
  a default one (containing bug in Java >= 1.8) will be used
  and further tests accessing space will fail (in Java >= 1.8).
 */
object GigaSpaceMacrosQueriesTest {

  var configurer: UrlSpaceConfigurer = _
  var gigaSpace: GigaSpace = _

  @BeforeClass
  def beforeClass() {
    configurer = new UrlSpaceConfigurer("/./testSpace")
    gigaSpace = new GigaSpaceConfigurer(configurer).create()
  }

  @AfterClass
  def afterClass() {
    if (configurer != null) {
      configurer.close()
    }
  }
}

class GigaSpaceMacrosQueriesTest {

  val date = new Date(1)
  
  def newMock: GigaSpace = {
    val gigaSpace = mock(classOf[GigaSpace])
    // compile each query to ensure its valid. otherwise an exception will be thrown
    when(gigaSpace.read(any(classOf[ISpaceQuery[_]]))).thenAnswer(new Answer[Person]() {
      def answer(invocation: InvocationOnMock): Person = {
        val sqlQuery = invocation.getArguments()(0).asInstanceOf[SQLQuery[Person]]
        val sqlParser = new SqlParser(new ByteArrayInputStream(sqlQuery.toString().getBytes()))
        sqlParser.parseStatement()
        // hacky way to get a hold of the sql query result
        Person(originalQuery = sqlQuery)
      }
    })
    gigaSpace
  }
  
  @Test
  def testQueries() {

    var gigaSpace: GigaSpace = null

    // test =
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.name == "john" }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "name = ?", "john"))

    // test <>
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.name != "john" }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "name <> ?", "john"))

    // test >
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.age > 10 }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "age > ?", 10: java.lang.Integer))

    // test >=
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.age >= 10 }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "age >= ?", 10: java.lang.Integer))
 
    // test <
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.age < 10 }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "age < ?", 10: java.lang.Integer))

    // test <=
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.age <= 10 }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "age <= ?", 10: java.lang.Integer))
    
    // test AND
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.age < 100 && p.age > 10 }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "( age < ? ) AND ( age > ? )", 100: java.lang.Integer, 10: java.lang.Integer))
    
    // test OR
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.age > 100 || p.age < 10 }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "( age > ? ) OR ( age < ? )", 100: java.lang.Integer, 10: java.lang.Integer))
  
    // test is null
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.name eq null }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "name is null", QueryResultType.OBJECT))

    // test is NOT null
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.name ne null }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "name is NOT null", QueryResultType.OBJECT))
    
    // test like
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.name like "%A" }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "name like '%A'", QueryResultType.OBJECT))
    
    // test NOT like
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.name notLike "%A" }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "name NOT like '%A'", QueryResultType.OBJECT))
    
    // test rlike
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.name rlike "a.*" }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "name rlike 'a.*'", QueryResultType.OBJECT))
    
    // test nested query
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.son.name == "john" }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "son.name = ?", "john"))
    
    // test date <
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.birthday < date }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "birthday < ?", date))

    // test date >
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.birthday > date }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "birthday > ?", date))

    // test date <
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.birthday <= date }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "birthday <= ?", date))

    // test date >
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.birthday >= date }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "birthday >= ?", date))
    
    // test object instantiation in RHS
    gigaSpace = newMock
    gigaSpace.predicate.read { p: Person => p.birthday == new Date(1) }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "birthday = ?", date))
    
    testSelect()
    
    testOrderBy()
    
    testOrderByAsc()
    
    testOrderByDesc()
    
    testGroupBy()
    
    testGroupByAndOrderBy()
  }
  
  private def testSelect() {
    var gigaSpace = newMock
    gigaSpace.predicate
    val mockPerson: Person = gigaSpace.predicate.read { p: Person =>
      select(p.name, p.son.age)
      p.son eq null
    }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "son is null", QueryResultType.OBJECT))
    val projections = mockPerson.originalQuery.getProjections()
    Assert.assertNotNull("missing projections", projections)
    Assert.assertEquals("wrong projections set", List("name", "son.age"), projections.toList)
  }
  
  private def testOrderBy() {
    var gigaSpace = newMock
    gigaSpace.predicate.read { p: Person =>
      orderBy(p.birthday, p.name)
      p.son eq null
    }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "son is null ORDER BY birthday,name", QueryResultType.OBJECT))
  }
  
  private def testOrderByAsc() {
    var gigaSpace = newMock
    gigaSpace.predicate.read { p: Person =>
      orderBy(p.birthday, p.name).ascending
      p.son eq null
    }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "son is null ORDER BY birthday,name ASC", QueryResultType.OBJECT))    
  }
  
  private def testOrderByDesc() {
    var gigaSpace = newMock
    gigaSpace.predicate.read { p: Person =>
      orderBy(p.birthday, p.name).descending
      p.son eq null
    }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "son is null ORDER BY birthday,name DESC", QueryResultType.OBJECT)) 
  }
  
  private def testGroupBy() {
    var gigaSpace = newMock
    gigaSpace.predicate.read { p: Person =>
      groupBy(p.birthday, p.name)
      p.son eq null
    }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "son is null GROUP BY birthday,name", QueryResultType.OBJECT))
  }
  
  private def testGroupByAndOrderBy() {
    var gigaSpace = newMock
    gigaSpace.predicate.read { p: Person =>
      orderBy(p.age)
      groupBy(p.birthday, p.name)
      p.son eq null
    }
    verify(gigaSpace).read(new SQLQuery(classOf[Person], "son is null GROUP BY birthday,name ORDER BY age", QueryResultType.OBJECT))
  }
  
}