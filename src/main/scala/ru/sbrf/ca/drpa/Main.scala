package ru.sbrf.ca.drpa

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object Main extends App with LazyLogging {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName(s"vaccin_happiness")
      .config("spark.master", "local")
      .getOrCreate()

  new DataLoader().load()

  MartLayer.run()

}
