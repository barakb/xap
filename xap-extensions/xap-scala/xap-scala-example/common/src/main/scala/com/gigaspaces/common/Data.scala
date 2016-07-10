package com.gigaspaces.common

import org.openspaces.scala.core.aliases.annotation._

import scala.beans.{BeanProperty, BooleanBeanProperty}

case class Data (
  @BeanProperty @SpaceId(autoGenerate = true) var id: String = null,
  @BeanProperty @SpaceRouting @SpaceProperty(nullValue = "-1") var `type`: Long = -1,
  @BeanProperty var rawData: String = null,
  @BeanProperty var data: String = null,
  @BooleanBeanProperty var processed: Boolean = false,
  @BooleanBeanProperty var verified: Boolean = false) {

  def this(`type`: Long, rawData: String) = this(null, `type`, rawData, null, false, false)

  def this() = this(-1, null)

  override def toString: String = s"id[${id}] type[${`type`}] rawData[${rawData}] data[${data}] processed[${processed}] verified[${verified}]"
}


case class Verification @SpaceClassConstructor() (
  @BeanProperty
  @SpaceId
  id: String,

  @BeanProperty
  dataId: String) extends scala.Serializable {

  override def toString: String = s"id[$id] dataId[$dataId]"
}