package es.hablapps.export

package object syntax {

  type ThrowableOrValue[+A] = Either[Throwable, A]

  case class InvalidStateException(message: String) extends Throwable {
    override def toString: String = message
  }
}
