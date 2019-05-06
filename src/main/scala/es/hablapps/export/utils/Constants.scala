package es.hablapps.export.utils

object Constants {

  lazy val BIND_VARIABLES: String = "/@bind/@"

  val `export.inputpath`: String = "export.inputpath"
  val `export.outputpath`: String = "export.outputpath"
  val `export.maps`: String = "export.maps"
  val `export.date.start`: String = "export.date.start"
  val `export.date.end`: String = "export.date.end"

  val `hive.server`: String = "hive.server"
  val `hive.port`: String = "hive.port"
  val `hive.principal`: String = "hive.principal"
  val `hive.query`: String = "hive.query"
  val `hive.auth`: String = "hive.auth"
  val `hive.params`: String = "hive.params"

  val `oracle.server`: String = "oracle.server"
  val `oracle.port`: String = "oracle.port"
  val `oracle.user`: String = "oracle.user"
  val `oracle.password`: String = "oracle.password"
  val `oracle.serviceName`: String = "oracle.serviceName"
  val `oracle.query`: String = "oracle.query"
  val `oracle.batchsize`: String = "oracle.batchsize"

  val `procedure.query`: String = "procedure.query"
  val `procedure.params`: String = "procedure.params"

}