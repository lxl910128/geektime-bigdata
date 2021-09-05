package club.projectgaia.bigdata.homework5

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 *         Created on 2021-09-05
 */

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(s"打印LogicalPlan:${plan.toJSON}")
    plan
  }
}

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    println("进入自定义扩展点")
    extensions.injectOptimizerRule { session => MyPushDown(session) }
  }
}
