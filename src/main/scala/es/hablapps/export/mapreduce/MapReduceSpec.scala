package es.hablapps.export.mapreduce

import es.hablapps.export.Export.ExportMapper
import es.hablapps.export.arg.{Arguments, Parameters}
import es.hablapps.export.rdbms.Rdbms.HiveDb
import es.hablapps.export.syntax.ThrowableOrValue
import es.hablapps.export.utils.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.security.UserGroupInformation
import cats.implicits._

case class MapReduceSpec(
                        startDate: String,
                        endDate:    String,
                        inputPath: String,
                        outputPath: String,
                        numMaps:    Int,
                        mapReduceParams: String
                        )(arg: Arguments) {
  def run(implicit hadoopConfig: Configuration): ThrowableOrValue[Int] = for {
    _ <- FileUtils.createFile(numMaps, startDate, endDate, inputPath, outputPath)
    jobConf <- createJobConf(mapReduceParams)(arg)
    job <- createJob(jobName = s"Export data to Exadata from ${startDate} to ${endDate}")(jobConf)
    _ <- addInputPathForJob(inputPath = inputPath)(job)
    _ <- setOutPutPathForJob(outputPath = outputPath)(job)
    exit <- waitForJobCompletion()(job = job)
    _ <- FileUtils.deleteDir(inputPath = inputPath, outputPath = outputPath)
  } yield exit


  private[this] def createJob(jobName: String)(jobConf: JobConf): ThrowableOrValue[Job] = Either.catchNonFatal {
    val j = Job.getInstance(jobConf, jobName)
      j.setMapperClass(classOf[ExportMapper])
      j.setNumReduceTasks(0)
      j.setOutputKeyClass(classOf[Text])
      j.setOutputValueClass(classOf[IntWritable])
    j
  }

  private[this] def createJobConf(mapReduceParams: String)(args: Arguments)(implicit hadoopConfig: Configuration): ThrowableOrValue[JobConf] = Either.catchNonFatal {

    val jobConf = new JobConf(hadoopConfig, classOf[ExportMapper])
    //FIXME Fix MapReduce parameters
//    mapReduceParams.foreach(p => {
//      jobConf.set(p._1, p._2.toString)
//    })

    //TODO Is this the best place to get the hive token??
    UserGroupInformation.setConfiguration(hadoopConfig)
    if(UserGroupInformation.isSecurityEnabled)
      for {
        _ <- HiveDb(args.hiveConfig.server,
          args.hiveConfig.port,
          args.hiveConfig.principal,
          "auth=kerberos")
          .setDelegationToken(jobConf.getCredentials)
      } yield()

    Parameters.setArguments(args)(jobConf)
  }

  private[this] def waitForJobCompletion(verbose: Boolean = true)(job: Job): ThrowableOrValue[Int] =
    Either.catchNonFatal {if (job.waitForCompletion(verbose)) 0 else 1}


  private[this] def addInputPathForJob(inputPath: String)(job: Job): ThrowableOrValue[Unit] =
    Either.catchNonFatal {
      FileInputFormat.addInputPath(job, new Path(inputPath))
    }

  private[this] def setOutPutPathForJob(outputPath: String)(job: Job): ThrowableOrValue[Unit] =
    Either.catchNonFatal { FileOutputFormat.setOutputPath(job, new Path(outputPath))}
}
