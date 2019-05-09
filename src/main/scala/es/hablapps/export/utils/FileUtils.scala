package es.hablapps.export.utils

import java.util.Calendar

import es.hablapps.export.syntax.ThrowableOrValue
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import cats.implicits._

object FileUtils { self =>
    private[this] def fs(implicit configuration: Configuration): FileSystem = FileSystem.get(configuration)

    def createFile(numMaps:Int, startDate:String, endDate: String, inputPath:String, outputPath: String)(implicit configuration: Configuration): ThrowableOrValue[Unit] =
      Either.catchNonFatal {
        self.deleteDir(inputPath, outputPath)

        val cal = Calendar.getInstance()
        cal.setTime(Utils.stringToTimeStamp(startDate))

        val diffDays: Long = Utils.diffDays(startDate, endDate)

        val numberPartitionPerMap = if ((diffDays / numMaps) == 0) 1 else  diffDays / numMaps

        var line: StringBuffer = null
        var indice = 0
        for (i <- 1 to numMaps) {
          for {
            fin <- self.createFile(s"$inputPath/input_$i")
          } yield {
            var j = 1

            line = new StringBuffer
            while (j <= numberPartitionPerMap && indice < diffDays) {
              val str = Utils.datetime_format.format(cal.getTime)
              line.append(s"$str ")
              cal.add(Calendar.DAY_OF_WEEK, 1)
              j += 1
              indice += 1
            }

            if (line.length()>0){
                fin.writeBytes(line.substring(0, line.length() - 1))
                fin.close()
              }
          }
        }


      }

    def deleteDir(inputPath: String, outputPath: String, recursive: Boolean = true)(implicit configuration: Configuration): ThrowableOrValue[Unit] =
      Either.catchNonFatal {
        fs.delete(new Path(inputPath),recursive)
        fs.delete(new Path(outputPath), recursive)
      }

    private[this] def createDir(inputPath: String, outputPath: String)(implicit configuration: Configuration): ThrowableOrValue[Unit] =
      Either.catchNonFatal {
        fs.create(new Path(inputPath), true)
        fs.create(new Path(outputPath), true)
      }

    private[this] def createFile(path: String)(implicit configuration: Configuration): ThrowableOrValue[FSDataOutputStream] =
      Either.catchNonFatal {
        fs.create(new Path(path))
      }

}
