package es.hablapps.export.security

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.Logger


object Kerberos {

    private val logger: Logger = Logger.getLogger("es.hablapps.export.security.Kerberos")

    def getRealUser: UserGroupInformation = {
      val currentUser: UserGroupInformation = UserGroupInformation.getCurrentUser
      Option(currentUser.getRealUser).getOrElse(currentUser)
    }

    def doAsRealUser[T](fn: => T): T = {
      val realUser: UserGroupInformation = getRealUser
      logger.info(s"Running function with user $realUser")
      try {
        realUser.doAs(new PrivilegedExceptionAction[T]() {
          override def run(): T = fn
        })
      } catch {
        case e: UndeclaredThrowableException => throw Option(e.getCause).getOrElse(e)
      }
    }


}
