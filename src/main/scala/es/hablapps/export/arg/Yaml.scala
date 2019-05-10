package es.hablapps.export.arg

import java.io.File

import scopt.OptionParser

case class Yaml(
                        configFile:File = null,
                        startDate: String = "",
                        endDate:   String = "",
                        mapReduce: Map[String, String] = Map.empty[String, String]
                        )

object Yaml {
  val parser: OptionParser[Yaml] = new OptionParser[Yaml]("HiveToOracle") {

    opt[File]('c', "configFile")
      .unbounded()
      .required()
      .text("Configuration File")
      .action((x, c) => c.copy(configFile = x))

    opt[String]('s', "startDate")
      .required()
      .text("Initial datetime")
      .action { (init, c) => c.copy(startDate = init) }

    opt[String]('e', "endDate")
      .required()
      .text("Finish datetime")
      .action { (finish, c) => c.copy(endDate = finish) }

    opt[Map[String, String]]('D', "mapReduce")
      .optional()
      .text("MapReduce parameters")
      .action { (wc, c) => c.copy(mapReduce = wc) }
  }

  def parse(args: Seq[String]): Yaml =
    parser.parse(args, Yaml()) match {
      case Some(conf) =>
        conf
      case None =>
        throw new RuntimeException(
          s"""|Missing configuration parameters
              |${parser.usage}""".stripMargin)
    }
}