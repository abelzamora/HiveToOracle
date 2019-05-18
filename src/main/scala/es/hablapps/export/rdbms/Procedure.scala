package es.hablapps.export.rdbms

import es.hablapps.export.syntax.ThrowableOrValue
import es.hablapps.export.utils.Utils
import org.apache.commons.lang.StringUtils

import scala.collection.immutable.IntMap

case class Procedure(
                    startDate: String,
                    endDate: String,
                    `procedure.query`: String,
                    `procedure.params`: String
                    )
//{
//
//  def run (implicit rdbms: Rdbms): ThrowableOrValue[Unit] = for {
//    _ <- rdbms.query(`procedure.query`, buildParams(startDate, endDate))
//    _ <- rdbms.closeConnection()
//  } yield()
//
//  private[this] def buildParams(startDate: String, endDate: String): IntMap[AnyRef] = {
//    IntMap[AnyRef](
//    1 -> StringUtils.remove(startDate, '-').asInstanceOf[AnyRef],
//            2 -> (Utils.diffDays(startDate, endDate) - 1).asInstanceOf[AnyRef])
//  }
//}


