package com.izzyacademy.product.analysis.jobs

trait JobService {
  def run(args: Array[String]): Unit
}
