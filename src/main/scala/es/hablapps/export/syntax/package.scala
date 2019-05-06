package es.hablapps.export

import scalaz.std.`try`._
import scalaz.{ValidationNel, \/, Success, Failure}

import scala.util.Try

package object syntax {

  type ThrowableOrValue[+A] = Throwable \/ A

  case class InvalidStateException(message: String) extends Throwable {
    override def toString: String = message
  }

  def nonNull[A, B](a: A, msg: String)(f: A => B): ValidationNel[Throwable, B] ={
    cata(Try(f(a)))(
      s => Success(s),
      e => Failure(if(msg.isEmpty) e else InvalidStateException(msg)).toValidationNel
    )
  }
}
