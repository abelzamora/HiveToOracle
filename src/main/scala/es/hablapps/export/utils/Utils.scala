package es.hablapps.export.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.util

import es.hablapps.export.utils.Constants._
import org.apache.commons.lang.StringUtils

import scala.collection.immutable.IntMap

object Utils { self =>

  val DATE_FORMAT = "yyyy-MM-dd"
  val datetime_format: SimpleDateFormat = new SimpleDateFormat(DATE_FORMAT)

  def stringToTimeStamp(date: String): Timestamp = new Timestamp(Utils.datetime_format.parse(date).getTime)
  def diffDays(startDate: String, endDate: String): Long = ChronoUnit.DAYS.between(stringToTimeStamp(startDate).toInstant, stringToTimeStamp(endDate).toInstant)

  def bindParametersReplacement(query:String, param: String): IntMap[AnyRef] = {
    IntMap(query.split(",").zipWithIndex.map(t => (t._2 + 1, bindReplacement[AnyRef](t._1, param))).toSeq: _*)
  }

  def bindReplacement[A](text: String, replacement:String): A = {
    if(replacement.isEmpty) text.asInstanceOf[A]
    else StringUtils.replace(text, BIND_VARIABLES, replacement).asInstanceOf[A]
  }

  def parsingParameterList(array: Any, sep: String): String = {
    if (null == array) ""
    else array.asInstanceOf[util.ArrayList[String]].toArray.mkString(sep)
  }


}

