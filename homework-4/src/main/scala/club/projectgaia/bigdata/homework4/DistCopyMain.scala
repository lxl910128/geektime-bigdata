package club.projectgaia.bigdata.homework4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 *         Created on 2021-08-18
 */
object DistCopyMain {
  def main(args: Array[String]): Unit = {
    // 传参检验
    if (args.length < 2 || args.length > 5) {
      println("参数错误，请重新输入")
    }
    var source: Path = null
    var target: Path = null
    var ignoreFailures = true
    var maxConcurrence = 3
    var maxFlag = false
    args.foreach(arg => {
      arg match {
        case "-i" => ignoreFailures = false
        case "-m" => maxFlag = true
        case parameter: String if maxFlag => {
          maxConcurrence = Integer.valueOf(parameter)
          maxFlag = false
        }
        case parameter: String if !maxFlag && source != null => target = new Path(parameter)
        case parameter: String if !maxFlag && source == null => source = new Path(parameter)
        case _ => {
          println("参数错误，请重新输入")
          System.exit(0)
        }
      }
    })

    // 先创建目录结构
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val fileList = new ListBuffer[(Path, Path)]
    if (!fs.exists(source) || !fs.exists(target)) {
      println("参数错误，请重新输入，源或目标文件加不存在")
    }
    createFile(source, target, fs, fileList)
    fs.close()

    // 使用spark 拷贝
    val spark = SparkSession.builder.master("master").appName("copy hdfs").getOrCreate()

    spark.sparkContext.parallelize(fileList, maxConcurrence).mapPartitions(it => {
      val fs = FileSystem.get(conf)
      val ret = it.map(sourceAndTarget => {
        try {
          val output = fs.create(sourceAndTarget._2)
          val input = fs.open(sourceAndTarget._1)
          IOUtils.copyBytes(input, output, 4096, true)
          s"拷贝${sourceAndTarget._1.getParent}/${sourceAndTarget._1.getName}到${sourceAndTarget._2.getParent}/${sourceAndTarget._2.getName}成功"
        } catch {
          case e: Throwable => s"拷贝${sourceAndTarget._1.getParent}/${sourceAndTarget._1.getName}到${sourceAndTarget._2.getParent}/${sourceAndTarget._2.getName}失败，${e.getMessage}"
        }
      })
      fs.close()
      ret
    }).collect().foreach(println)
    spark.stop()
  }

  def createFile(sourceFs: Path, targetFs: Path, fs: FileSystem, fileList: ListBuffer[(Path, Path)]): Unit = {
    val iterator = fs.listFiles(sourceFs, true)
    while (iterator.hasNext) {
      val fileStatus = iterator.next()
      val newTargetFs = targetFs.suffix("/" + fileStatus.getPath.getName)
      if (fileStatus.isDirectory) {
        fs.mkdirs(newTargetFs)
        createFile(fileStatus.getPath, newTargetFs, fs, fileList)
      } else {
        fileList += Tuple2(fileStatus.getPath, newTargetFs)
      }
    }
  }

}
