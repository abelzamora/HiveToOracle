package es.hablapps.export

package object domain {

  sealed trait ThrowableError

  final case class ConnectionError(t: Throwable) extends ThrowableError

  final case class QueryError(t: Throwable) extends ThrowableError

  final case class BatchError(t: Throwable) extends ThrowableError

}
