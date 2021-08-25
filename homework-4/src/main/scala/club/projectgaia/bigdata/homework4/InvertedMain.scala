package club.projectgaia.bigdata.homework4

import org.apache.spark.sql.SparkSession

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 *         Created on 2021-08-17
 */
object InvertedMain {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    val stringWithIndexSeq = Seq(("it is what it is", 0), ("what is it", 1), ("it is a banana", 2))
    spark.sparkContext.parallelize(stringWithIndexSeq)
      .flatMap(v => {
        v._1.split(" ").map((_, Array(v._2)))
      }).reduceByKey((ary1, ary2) => ary1 ++ ary2)
      .collect()
      .foreach(value => {
        println(s"key: ${value._1}  value: ${value._2.distinct.mkString(";")} ")
      })

    spark.sparkContext.parallelize(stringWithIndexSeq)
      .flatMap(v => {
        v._1.split(" ").map(word => ((word, v._2), 1))
      }).reduceByKey(_ + _)
      .map(v => (v._1._1, Array((v._1._2, v._2))))
      .reduceByKey((ary1, ary2) => ary1 ++ ary2)
      .collect()
      .foreach(value => println(s"key: ${value._1} value ${value._2.mkString(";")}"))
    spark.stop()
  }
}
