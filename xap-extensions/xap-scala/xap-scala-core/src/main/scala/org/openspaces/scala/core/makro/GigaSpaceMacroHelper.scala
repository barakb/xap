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

import com.gigaspaces.async.AsyncFutureListener
import com.gigaspaces.client.{ChangeResult, ChangeSet, SpaceProxyOperationModifiers}
import com.j_spaces.core.client.SQLQuery

import scala.annotation.tailrec
import scala.compat.Platform.EOL
import scala.language.postfixOps

/**
 * @since 9.6
 * @author Dan Kilman
 */
abstract class ReadTakeMultipleMacroHelper extends GigaSpaceMacroHelper {
  
  val take: Boolean
  
  override protected def getInvocationParams(sqlQueryTree: c.Tree): List[c.Tree] = {
    (maxEntriesOption, modifiersOption) match {
      case (None, None) => List(sqlQueryTree)
      case (Some(maxEntriesExpr), None) => List(sqlQueryTree, maxEntriesExpr.tree)
      case (Some(maxEntriesExpr), Some(modifiersExpr)) => List(sqlQueryTree, maxEntriesExpr.tree, modifiersExpr.tree)
      case _ => throw new IllegalStateException("Illegal method call")
    }
  }
  
  override protected def getInvocationMethodName: String = (if (take) "take" else "read") + "Multiple"
  
}

/**
 * @since 9.6
 * @author Dan Kilman
 */
abstract class ReadTakeMacroHelper extends GigaSpaceMacroHelper {
  
  val ifExists: Boolean
  val take: Boolean
  
  override protected def getInvocationParams(sqlQueryTree: c.Tree): List[c.Tree] = {
    (timeoutOption, modifiersOption) match {
      case (None, None) => List(sqlQueryTree)
      case (Some(timeoutExpr), None) => List(sqlQueryTree, timeoutExpr.tree)
      case (Some(timeoutExpr), Some(modifiersExpr)) => List(sqlQueryTree, timeoutExpr.tree, modifiersExpr.tree)
      case _ => throw new IllegalStateException("Illegal method call")
    }
  }
  
  override protected def getInvocationMethodName: String = {
    val prefix = if (take) "take" else "read"
    if (ifExists) prefix + "IfExists" else prefix
  }
}

/**
 * @since 9.6
 * @author Dan Kilman
 */
abstract class CountClearMacroHelper extends GigaSpaceMacroHelper {
  
  val clear: Boolean
  
  override protected def getInvocationParams(sqlQueryTree: c.Tree): List[c.Tree] = {
    modifiersOption match {
      case None => List(sqlQueryTree)
      case Some(modifiersExpr) => List(sqlQueryTree, modifiersExpr.tree)
    }
  }
  
  override protected def getInvocationMethodName: String = if (clear) "clear" else "count"
  
}

/**
 * @since 9.6
 * @author Dan Kilman
 */
abstract class ChangeMacroHelper extends GigaSpaceMacroHelper {
  
  val changeSetExpr: c.Expr[ChangeSet]
  
  override protected def getInvocationParams(sqlQueryTree: c.Tree): List[c.Tree] = {
    val changeSetTree = changeSetExpr.tree
    (timeoutOption, modifiersOption) match {
      case (None, None) => List(sqlQueryTree, changeSetTree)
      case (Some(timeoutExpr), None) => List(sqlQueryTree, changeSetTree, timeoutExpr.tree)
      case (None, Some(modifiersExpr)) => List(sqlQueryTree, changeSetTree, modifiersExpr.tree)
      case (Some(timeoutExpr), Some(modifiersExpr)) => List(sqlQueryTree, changeSetTree, modifiersExpr.tree, timeoutExpr.tree)
    }
  }
  
  override protected def getInvocationMethodName: String = "change"
  
}

/**
 * @since 9.6
 * @author Dan Kilman
 */
abstract class AsyncReadTakeMacroHelper[T] extends GigaSpaceMacroHelper {
  
  val take: Boolean
  val futureListenerOption: Option[c.Expr[AsyncFutureListener[T]]] = None
  
  override protected def getInvocationParams(sqlQueryTree: c.Tree): List[c.Tree] = {
    (timeoutOption, modifiersOption, futureListenerOption) match {
      case (None, None, None) => List(sqlQueryTree)
      case (None, None, Some(listener)) => List(sqlQueryTree, listener.tree)
      case (Some(timeout), None, None) => List(sqlQueryTree, timeout.tree)
      case (Some(timeout), None, Some(listener)) => List(sqlQueryTree, timeout.tree, listener.tree)
      case (Some(timeout), Some(modifiers), None) => List(sqlQueryTree, timeout.tree, modifiers.tree)
      case (Some(timeout), Some(modifiers), Some(listener)) => List(sqlQueryTree, timeout.tree, modifiers.tree, listener.tree)
      case _ => throw new IllegalStateException("Illegal method call")
    }
  }
  
  override protected def getInvocationMethodName: String = "async" + (if (take) "Take" else "Read")
}

/**
 * @since 9.6
 * @author Dan Kilman
 */
abstract class AsyncChangeMacroHelper[T] extends GigaSpaceMacroHelper {
  
  val changeSetExpr: c.Expr[ChangeSet]
  val futureListenerOption: Option[c.Expr[AsyncFutureListener[ChangeResult[T]]]] = None
  
  override protected def getInvocationParams(sqlQueryTree: c.Tree): List[c.Tree] = {
    val changeSetTree = changeSetExpr.tree
    (timeoutOption, modifiersOption, futureListenerOption) match {
      case (None, None, None) => List(sqlQueryTree, changeSetTree)
      case (None, None, Some(listener)) => List(sqlQueryTree, changeSetTree, listener.tree)
      case (Some(timeout), None, None) => List(sqlQueryTree, changeSetTree, timeout.tree)
      case (Some(timeout), None, Some(listener)) => List(sqlQueryTree, changeSetTree, timeout.tree, listener.tree)
      case (None, Some(modifiers), None) => List(sqlQueryTree, changeSetTree, modifiers.tree)
      case (None, Some(modifiers), Some(listener)) => List(sqlQueryTree, changeSetTree, modifiers.tree, listener.tree)
      case (Some(timeout), Some(modifiers), None) => List(sqlQueryTree, changeSetTree, modifiers.tree, timeout.tree)
      case (Some(timeout), Some(modifiers), Some(listener)) => List(sqlQueryTree, changeSetTree, modifiers.tree, timeout.tree, listener.tree)
    }
  }
  
  override protected def getInvocationMethodName: String = "asyncChange"
}

/**
 * @since 9.6
 * @author Dan Kilman
 */
object GigaSpaceMacroHelper {
  def createQuery[T](typeName: String, query: String, params: List[Object]) =
    new SQLQuery[T](typeName, query, com.gigaspaces.query.QueryResultType.OBJECT, params:_*)
}

/**
 * @since 9.6
 * @author Dan Kilman
 */
abstract class GigaSpaceMacroHelper {
  
  val c: GigaSpaceMacros.TypedContext
  import c.universe._

  private val selectDirective = "select"
  private val groupByDirective = "groupBy"
  private val orderByDirective = "orderBy"
  private val orderByAscendingDirective = "ascending"
  private val orderByDescendingDirective = "descending"
    
  private val eqOp = "is"
  private val neOp = "is NOT"
  private val likeOp = "like"
  private val notLikeOp = "NOT like"
  private val rlikeOp = "rlike"
 
  protected val timeoutOption: Option[c.Expr[Long]] = None
  protected val modifiersOption: Option[c.Expr[SpaceProxyOperationModifiers]] = None
  protected val maxEntriesOption: Option[c.Expr[Int]] = None
    
  protected def getInvocationParams(sqlQueryTree: c.Tree): List[c.Tree]
  
  protected def getInvocationMethodName: String
  
  def generate[T](predicate: c.Expr[T => Boolean]): c.Tree = {
    
    try {
      val (paramName, typeName, body) = extractParameterNameTypeNameAndApplyTree(predicate)
      body match {
        case Block(statements, rawExpression) => {
          val (selectProperties, orderByProperties, orderByDirection, groupByProperties) = 
            processStatements(statements, paramName.toString)
          processExpression(paramName, 
                            typeName, 
                            rawExpression, 
                            selectProperties, 
                            orderByProperties, 
                            orderByDirection,
                            groupByProperties)
        }
        case _ => processExpression(paramName, typeName, body) 
      }
    } catch {
      case e: Throwable => {
        c.error(c.enclosingPosition, e.getStackTrace.mkString("", EOL, EOL))
        throw e
      }
    }
  }
 
  private def processStatements(statements: List[Tree], paramName: String) = {
    
    var selectProperties: List[String] = null
    var orderByProperties: List[String] = null
    var orderByDirection: String = null
    var groupByProperties: List[String] = null
    
    statements map(removeFullyQualifiedPackageNames) foreach { _ match {
      case Select(Apply(Ident(directive), params), subDirective) 
        if directive.toString == orderByDirective => {
        if (orderByProperties != null) {
          c.abort(c.enclosingPosition, "cannot place more then 1 orderBy directive")
        }
        val orderByDirectionOption = convertToOrderByDirection(subDirective)
        if (orderByDirectionOption.isEmpty) {
          c.abort(c.enclosingPosition, "Illegal orderBy direction: " + subDirective)
        }
        orderByDirection = orderByDirectionOption.get
        orderByProperties = extractDirectivePropertyNames(params, paramName, orderByDirective)
      }
      case Apply(Ident(directive), params) => {
        if (directive.toString == selectDirective) {
          if (selectProperties != null) {
            c.abort(c.enclosingPosition, "cannot place more then 1 select directive")
          }
          selectProperties = extractDirectivePropertyNames(params, paramName, selectDirective)
        } else if (directive.toString == orderByDirective) {
          if (orderByProperties != null) {
            c.abort(c.enclosingPosition, "cannot place more then 1 orderBy directive")
          }
          orderByProperties = extractDirectivePropertyNames(params, paramName, orderByDirective)
        } else if (directive.toString == groupByDirective) {
          if (groupByProperties != null) {
            c.abort(c.enclosingPosition, "cannot place more then 1 groupBy directive")
          }
          groupByProperties = extractDirectivePropertyNames(params, paramName, groupByDirective)
        } else {
          c.abort(c.enclosingPosition, "unknown directive: " + directive)
        }
      }
      case other => c.abort(c.enclosingPosition, "bad statement: " + showRaw(other))
    }}
    
    (selectProperties, orderByProperties, orderByDirection, groupByProperties)
  }
  
  private def extractDirectivePropertyNames(params: List[Tree], paramName: String, directive: String) = {
    if (params.isEmpty) {
      c.abort(c.enclosingPosition, "cannot pass zero arguments to " + directive + " directive")
    }
    params.map { param =>
      val paramString = param.toString
      if (!paramString.startsWith(paramName + ".")) {
        throw new Exception(s"parameter name must begin with '$paramName' reason: ${paramString}")
      } else {
          paramString.substring(paramName.length + 1)
      }
    }
  }
  
  private def processExpression[T](
      paramName: TermName, 
      typeName: String, 
      rawExpression: Tree,
      selectProperties: List[String] = null,
      orderByProperties: List[String] = null,
      orderByDirection: String = null,
      groupByProperties: List[String] = null) = {

    val expression = removeFullyQualifiedPackageNames(rawExpression)

    // build query and parameter list to build new SQLQuery
    val paramsBuffer = collection.mutable.ArrayBuffer[Tree]()
    val query: String = {
      val initialQuery = visitExpressionTree(true, paramName.toString, expression, paramsBuffer).query
      addDirectivesToQueryIfNeeded(initialQuery, orderByProperties, orderByDirection, groupByProperties)
    }
    val params = paramsBuffer.map(box) toList
    
    // build String trees for type name and query
    val typeNameStringTree = toStringTree(typeName)
    val queryStringTree = toStringTree(query)

    // build 'new SQLQuery(...)' tree
    val sqlQueryExpr = createPartialSqlQueryTree(typeNameStringTree, 
                                                 queryStringTree, 
                                                 setProjections = selectProperties != null)
    val sqlQueryTree = createFinalSQLQuery(sqlQueryExpr.tree, 
                                           params, 
                                           typeName,
                                           selectProperties)
    
    // build the operation invocation tree and pass the new sqlQueryExpr
    // gigaSpace.read(new SQLQuery(typeName, query, QueryResultType.OBJECT, param1, param2, ..., paramN))
    // final result is something like this:
    createFinalInvocationTree(sqlQueryTree)
  }
  
  private def extractParameterNameTypeNameAndApplyTree(predicate: c.Expr[_]) = {
    val c.Expr(Function(ValDef(_, paramName, tpe, _)::Nil, body)) = predicate
    (paramName, tpe.toString, body)
  }
  
  private def removeFullyQualifiedPackageNames(apply: c.Tree): c.Tree = {
    val implicitRemoverTransformer = new Transformer {
      override val treeCopy = newStrictTreeCopier
      override def transform(t: Tree) = {
        super.transform(t) match {
          // for directives in statements 
          case Apply(Select(Select(_, macroDirectiveNameTypeName), directive), params) 
            if macroDirectiveNameTypeName.toString == "MacroDirectives" => {
              Apply(Ident(directive), params)
          }
          // for implicits in final final expression
          case Apply(Select(Apply(Select(Select(_,gigaSpaceImplicitsTypeName), _), lhs::Nil), operator), rhs::Nil) 
            if gigaSpaceImplicitsTypeName.toString == "ScalaGigaSpacesImplicits" => {
              Apply(Select(lhs, operator), rhs::Nil)
          }
          case other => other
        }
      }
    }
    implicitRemoverTransformer.transform(apply)
  }
  
  private def toStringTree(string: String): c.Tree = q"$string"

  private def createPartialSqlQueryTree(
      typeNameStringTree: c.Tree, 
      queryStringTree: c.Tree,
      setProjections: Boolean) = {
    if (setProjections) {
      reify { 
        GigaSpaceMacroHelper.createQuery[AnyRef](
          c.Expr[String](typeNameStringTree).splice, 
          c.Expr[String](queryStringTree).splice, 
          List[Object]("#PLACEHOLDER#")
        ).setProjections(null)
      }      
    } else {
      reify { 
        GigaSpaceMacroHelper.createQuery[AnyRef](
          c.Expr[String](typeNameStringTree).splice, 
          c.Expr[String](queryStringTree).splice, 
          List[Object]("#PLACEHOLDER#")
        )
      }       
    }
    

  }
  
  private def addDirectivesToQueryIfNeeded(query: String,
                                           orderByProperties: List[String],
                                           orderByDirection: String,
                                           groupByProperties: List[String]): String = {
    val finalQuery = new StringBuilder(query)
    if (groupByProperties != null) {
      finalQuery.append(" GROUP BY ").append(groupByProperties.mkString(","))
    }
    if (orderByProperties != null) {
      finalQuery.append(" ORDER BY ").append(orderByProperties.mkString(","))
      if (orderByDirection != null) {
        finalQuery.append(" ").append(orderByDirection)
      }
    }
    finalQuery.toString
  }
  
  private def createFinalSQLQuery(
      partialSqlQueryTree: c.Tree, 
      params: List[c.Tree], 
      typeName: String,
      selectParams: List[String]): c.Tree = {
    val sqlQueryTransformer = new Transformer {
      override val treeCopy = newStrictTreeCopier
      override def transform(t: Tree) = {
        super.transform(t) match {
          case Apply(select, typeName::query::listApply::Nil) => {
            val Apply(typeApply, placeHolderParams) = listApply
            val applyParams = Apply(typeApply, params)
            Apply(select, typeName::query::applyParams::Nil)
          }
          case Ident(name) if name.toString == "AnyRef" => {
            createTreeFromTypeName(typeName)
          }
          case Apply(Select(select, methodCall), Literal(Constant(null))::Nil) 
            if methodCall.toString == "setProjections" => {
            val selectParamsTree = selectParams map { selectParam => q"$selectParam" }
            Apply(Select(select, methodCall), selectParamsTree)
          }
          case other => other
        }
      }
    }
    sqlQueryTransformer.transform(partialSqlQueryTree)
  }
  
  private def createTreeFromTypeName(typeName: String) = {
    @tailrec
    def helper(currentTree: Tree, names: List[String]): Tree = {
      (names: @unchecked) match {
        case last::Nil => Select(currentTree, TypeName(last))
        case first::rest => helper(Select(currentTree, TermName(first)), rest)
      }
    }
    val splitName = typeName.split('.')
    if (splitName.length == 1) {
      Ident(TypeName(typeName))
    } else {
        val names = splitName.toList
        helper(Ident(TermName(names.head)), names.tail)
    }
  }
  
  protected def createFinalInvocationTree(sqlQueryTree: c.Tree): c.Tree = {
    val c.Expr(gigaSpaceTree) = reify { c.prefix.splice.gigaSpace }
    Apply(Select(gigaSpaceTree, TermName(getInvocationMethodName)), getInvocationParams(sqlQueryTree))
  }
  
  case class VisitResult(query: String, isLeaf: Boolean)
  
  private def visitExpressionTree
    (isLhs: Boolean, 
     paramName: String, 
     tree: c.Tree,
     params: collection.mutable.ArrayBuffer[c.Tree],
     op: String = null): VisitResult = {
     
    def processComplexQuery(lhs: Tree, sqlOperator: String, rhs: Tree): VisitResult = {
      
      val lhsVisitResult = visitExpressionTree(true, paramName, lhs, params)
      val rhsVisitResult = visitExpressionTree(false, paramName, rhs, params, op = sqlOperator)
      
      val stringLhs = buildStringFromVisitResult(lhsVisitResult)
      val stringRhs = buildStringFromVisitResult(rhsVisitResult)
        
      VisitResult(stringLhs + " " + sqlOperator + " " + stringRhs, false)
    }
    
    def processLeafQuery: VisitResult = {
      val stringTree = tree.toString
      if (isLhs) {
        if (!stringTree.startsWith(paramName + ".")) {
          throw new Exception(s"Left hand side parameter name must begin with '$paramName' reason: ${stringTree}")
        } else {
          VisitResult(stringTree.substring(paramName.length + 1), true)
        }
      } else {
        if ((op eq eqOp) || (op eq neOp)) {
          if (!("null" == stringTree)) {
            throw new Exception("can only use `eq` or `ne` with `null`")
          }
          VisitResult(stringTree, true)
        } else if ((op eq likeOp) || (op eq notLikeOp) || (op eq rlikeOp)) {
          tree match {
            case Literal(Constant(theString: String)) => VisitResult(s"'${theString}'", true) 
            case _ => throw new Exception(s"'${op}' can only be applied on strings")
          }
        } else {
          params += tree
          VisitResult("?", true)
        }
      }
    }
    
    tree match {
      case Apply(Select(lhs, operator), rhs::Nil) if !convertToSqlOperator(operator).isEmpty => {
        processComplexQuery(lhs, convertToSqlOperator(operator).get, rhs)
      }
      case _ => {
        processLeafQuery
      }
    }
  }
  
  private def buildStringFromVisitResult(visitResult: VisitResult): String = {
    var result = visitResult.query
    if (!visitResult.isLeaf)
      result = "( " + result + " )"
    result
  }
  
  private def box(t: c.Tree): c.Tree = {
    Apply(Select(Select(Select(Select(Select(Ident(TermName("org")),
         TermName("openspaces")), TermName("core")),
         TermName("util")), TermName("Boxer")), TermName("box")), List(t))
  }
  
  private def convertToSqlOperator(treeOperator: Name): Option[String] = {
    treeOperator.toString match {
      case "$eq$eq"      => Some("=")
      case "$bang$eq"    => Some("<>")
      case "$greater"    => Some(">")
      case "$greater$eq" => Some(">=")
      case "$less"       => Some("<")
      case "$less$eq"    => Some("<=")
      case "$amp$amp"    => Some("AND")
      case "$bar$bar"    => Some("OR")
      case "eq"          => Some(eqOp)
      case "ne"          => Some(neOp)
      case "like"        => Some(likeOp)
      case "notLike"     => Some(notLikeOp)
      case "rlike"       => Some(rlikeOp)
      case _             => None
    }
  }
 
  private def convertToOrderByDirection(orderByDirective: Name): Option[String] = {
    orderByDirective.toString match {
      case `orderByAscendingDirective`  => Some("ASC")
      case `orderByDescendingDirective` => Some("DESC")
      case _                            => None
    }
  }
  
}