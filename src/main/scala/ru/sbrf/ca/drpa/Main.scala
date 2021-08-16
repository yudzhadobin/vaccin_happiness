package ru.sbrf.ca.drpa

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object Main extends App with LazyLogging {

  logger.info("Hello world")

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName(s"vaccin_happiness")
      .config("spark.master", "local")
      .getOrCreate()

  logger.info("Spark session is created")

  new DataLoader().load()

  logger.info("Data is loaded")

  BusinessLayer.run()

  logger.info("BusinessLayer is successfully run")

  BusinessLayer.run()

  logger.info("BusinessLayer is successfully run")

  MartLayer.run()

  logger.info("MartLayer is successfully run")

}
