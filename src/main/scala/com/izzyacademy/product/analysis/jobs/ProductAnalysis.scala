package com.izzyacademy.product.analysis.jobs

import com.izzacademy.product.analysis.models.{ProductDetails, ProductEnriched}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

class ProductAnalysis() extends JobService {

  override def run(args: Array[String]): Unit = {

    //val mongoDBURI = "mongodb://admin@mongo-internal.mongo.svc.cluster.local:27017/admin"

    val mongoDBURI = "mongodb://admin:mong044120@52.241.138.173:27017/admin"
    val databaseName = "ecommerce"

    val sparkConfig = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .setAppName(this.getClass().getName())

    // https://docs.mongodb.com/spark-connector/master/configuration#std-label-spark-input-conf
    // https://docs.mongodb.com/spark-connector/master/configuration#std-label-spark-output-conf
    val currentSparkSession = SparkSession.builder()
      .config(sparkConfig)
      .config("spark.mongodb.input.uri", mongoDBURI)
      .config("spark.mongodb.output.uri", mongoDBURI)
      .getOrCreate()

    // This is necessary so as to find encoders for types stored in a Datasets.
    // Primitive types (Int, String, etc) and Product types (case classes) are supported by importing currentSparkSession.implicits._
    import currentSparkSession.implicits._

    val productsMap = Map(
      "uri" -> mongoDBURI,
      "database" -> databaseName,
      "collection" -> "products"
    )

    val productDetailsMap = Map(
      "uri" -> mongoDBURI,
      "database" -> databaseName,
      "collection" -> "product_details"
    )

    val productsEnrichedMap = Map(
      "uri" -> mongoDBURI,
      "database" -> databaseName,
      "collection" -> "products_enriched"
    )

    // ReadConfig
    // https://docs.mongodb.com/spark-connector/master/scala/read-from-mongodb#std-label-gs-read-config
    val readConfigProducts = ReadConfig(productsMap)
    val readConfigProductDetails = ReadConfig(productDetailsMap)

    // WriteConfig
    //
    val writeConfigProductsEnriched = WriteConfig(productsEnrichedMap)

    // Use ReadConfig to Load and WriteConfig to Save
    // https://docs.mongodb.com/spark-connector/master/configuration#std-label-spark-input-conf
    val productsDs = MongoSpark.load(currentSparkSession, readConfigProducts)
      .select("product_id", "department", "name")
      //.as[Product] convert to DataSet

    val productDetailsDs = MongoSpark.load(currentSparkSession, readConfigProductDetails)
      .select("product_id", "long_description")
      //.as[ProductDetails] convert to DataSet


    val enrichmentResults: Dataset[ProductEnriched] = productsDs
      .join(productDetailsDs, productsDs("product_id") === productDetailsDs("product_id"))
      .select(productsDs("product_id"), productsDs("department"), productsDs("name"), productDetailsDs("long_description"))
      .sort(col("product_id").asc)
      .as[ProductEnriched] // convert to DataSet

    productsDs.printSchema();
    productDetailsDs.printSchema()

    productsDs.show()
    productDetailsDs.show()

    enrichmentResults.show()

    // Saving dataset to MongoDB Collection
    MongoSpark.save(enrichmentResults, writeConfigProductsEnriched)

    currentSparkSession.stop()
  }
}
