package es.hablapps.export

package object syntax {

  type ThrowableOrValue[+A] = Either[Throwable, A]

}
