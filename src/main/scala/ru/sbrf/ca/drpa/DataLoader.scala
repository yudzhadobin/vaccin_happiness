package ru.sbrf.ca.drpa

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{SaveMode, SparkSession}

class DataLoader extends LazyLogging {

  def load()(implicit session: SparkSession): Unit = {
    readAndSave("./src/main/resources/data/happiness/2019.csv", "happiness_2019")
    readAndSave("./src/main/resources/data/vaccination/country_vaccinations.csv", "country_vaccinations")
    readAndSave("./src/main/resources/data/vaccination/country_vaccinations_by_manufacturer.csv", "country_vaccinations_by_manufacturer")
  }

  private def readAndSave(path: String, tableName: String)(implicit session: SparkSession): Unit = {
    val df = session.read
      .option("header", true)
      .option("inferSchema", "true")
      .csv(path)
    logger.info(s"File path: $path is loaded")
    df.show()

    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    logger.info(s"Table: $tableName is created")
  }
}
