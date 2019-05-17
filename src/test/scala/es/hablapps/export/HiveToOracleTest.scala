package es.hablapps.export

import java.io.File

import es.hablapps.export.arg.{AppConfig, Arguments, Yaml}
import es.hablapps.export.rdbms.Rdbms.{HiveDb, OracleDb}
import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable.IntMap
import pureconfig.Derivation._
import pureconfig.generic.auto._

//TODO It is necessary to test every single functionality
class HiveToOracleTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  private val logger: Logger = Logger.getLogger(classOf[HiveToOracleTest])
  lazy val inputFile: String = new File(classOf[HiveToOracleTest].getClassLoader.getResource("application.conf").getFile).getAbsolutePath

//  "Yaml parser" should "read the values an create a new object" in {
//
//    case class Foo(foo: String, bar: String)
//    import scala.collection.JavaConverters._
//
//    val yaml =
//      """
//        |hive:
//        | foo: Foo
//        | bar: Bar
//        |
//        |oracle:
//        | foo: Fii
//        | bar: Bir
//      """.stripMargin
//
//    val obj: mutable.Map[String, Any] = new YamlT().load(yaml).asInstanceOf[java.util.Map[String, Any]].asScala
//
//
//    val h: mutable.Map[String, Any] = obj("hive").asInstanceOf[java.util.Map[String, Any]].asScala
//    val o: mutable.Map[String, Any] = obj("oracle").asInstanceOf[java.util.Map[String, Any]].asScala
//
//    val hive = Foo(h("foo").toString, h("bar").toString)
//    val oracle = Foo(o("foo").toString, o("bar").toString)
//
//    hive.foo shouldBe "Foo"
//    hive.bar shouldBe "Bar"
//
//    oracle.foo shouldBe "Fii"
//    oracle.bar shouldBe "Bir"
//
//  }
//
//  "Argot Parser" should "parse mapreduce arguments" in {
//    val argsOK: Array[String] = Array[String](inputFile, "2019-01-01", "2019-01-02", "-Dmapreduce.job.ubertask.enable=true", "-Dmapreduce.task.timeout=3600000")
//    val argsKO: Array[String] = Array[String](inputFile, "2019-01-01", "2019-01-02", "-Xmapreduce.job.ubertask.enable=true", "-Dmapreduce.task.timeout=3600000")
//
//    for {
//      arg <- Yaml(argsOK).run
//    } yield {
//      arg.startDate shouldBe "2019-01-01"
//      arg.endDate shouldBe "2019-01-02"
//
//      arg.oracleConfig shouldBe OracleConfig(
//        "localhost",
//        1521,
//        "system",
//        "oracle",
//        "xe",
//        "INSERT /*+ APPEND_VALUES */ INTO students VALUES",
//        10
//      )
//
//      arg.hiveConfig shouldBe HiveConfig(
//        "127.0.0.1",
//        10000,
//        "",
//        "",
//        "select * from default.students_partitioned where fec_registro >= ? and fec_registro <= ?",
//        "/@bind/@,/@bind/@"
//      )
//    }
//
//    an [org.clapper.argot.ArgotUsageException] should be thrownBy Yaml(argsKO).run
//
//  }


  "HiveToracle" should "load some data" in {
    lazy val args = Array[String](
      s"--configFile=$inputFile",
      "--startDate=2019-01-01",
      "--endDate=2019-01-02",
      "--mapReduce=mapreduce.job.ubertask.enable=true"
    )

//    val yamlConfig = Yaml.parse(args)
//    val arg: Arguments = AppConfig.read[Arguments](yamlConfig.configFile.toPath, "hiveToOracle")

    //val oracleDb: OracleDb = OracleDb(arg.oracleConfig)
//    val hiveDb: HiveDb = HiveDb(arg.hiveConfig.server, arg.hiveConfig.port, arg.hiveConfig.principal, arg.hiveConfig.auth)

    val countHive: Int = 0///hiveDb.query("select count(1) from students", IntMap.empty[AnyRef]).map(rs => {rs.next(); rs.getInt(1)}).getOrElse(0)
    //oracleDb.query("truncate table students", IntMap.empty[AnyRef])

    Export.main(args)

    val countOracle: Int = 0//oracleDb.query("select count(1) from students", IntMap.empty).map(rs => {rs.next(); rs.getInt(1)}).getOrElse(0)

    logger.info(s"Hive transactions: $countHive | Oracle transactions: $countOracle")

    countHive shouldEqual countOracle

  }

}
