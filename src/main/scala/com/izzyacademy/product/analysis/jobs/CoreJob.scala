package com.izzyacademy.product.analysis.jobs

object CoreJob {

  def main(args: Array[String]): Unit = {

    val serverClass="com.izzyacademy.product.analysis.jobs.ProductAnalysis"

    val service:JobService = Class.forName(serverClass).newInstance().asInstanceOf[JobService];

    service.run(args);
  }
}
