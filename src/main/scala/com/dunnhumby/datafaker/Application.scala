
package com.dunnhumby.datafaker

import com.github.javafaker.Faker
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application extends App {

  Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

  val parsedArgs = ArgsParser.validateArgs(ArgsParser.parseArgs(args.toList))
  val conf = new SparkConf()
    .set("spark.ui.showConsoleProgress", "true")
    .setAppName("data-faker")
  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

//  val fake = udf((e: String) => new Faker().expression(e))
  spark.udf.register("fake", (e: String) => new Faker().expression(e))
  spark.sql(s"create database if not exists ${parsedArgs("database")}")

  val schema = YamlParser.parseSchemaFromFile(parsedArgs("file"))
  val dataGenerator = new DataGenerator(spark, parsedArgs("database"))

  dataGenerator.generateAndWriteDataFromSchema(schema)
}
