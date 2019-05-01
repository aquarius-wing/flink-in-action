package io.miaoji.blog.flinkinaction.job

import org.apache.flink.streaming.api.scala._


/**
  * 包: io.miaoji.blog.flinkinaction.job
  * 开发者: wing
  * 开发时间: 2019-05-01
  * 功能：热词统计
  */
object HotWordStatisticJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val currentTimeStamp = System.currentTimeMillis()
    val dataStream = env.fromElements[(Long, String)](
      (currentTimeStamp,"word1"),
      (currentTimeStamp+1,"word2"),
      (currentTimeStamp+2,"word1"),
      (currentTimeStamp+3,"word3")
    )

    dataStream
      .map(x => (x._1, x._2, 1))
      .keyBy(1)
      .sum(2)
      .print()

    env.execute("Hot Word Statistic Job")
  }

}
