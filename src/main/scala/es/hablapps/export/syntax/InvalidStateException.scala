package es.hablapps.export.syntax

case class InvalidStateException(message: String) extends Throwable {
  override def toString: String = message
}
