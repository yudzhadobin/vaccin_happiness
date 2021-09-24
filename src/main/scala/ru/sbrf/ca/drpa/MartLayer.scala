package ru.sbrf.ca.drpa

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{SaveMode, SparkSession}

object MartLayer {

  def run()(implicit session: SparkSession): Unit = {
    val happiness_df = session.sql("SELECT * FROM happiness_2019")
    val country_vaccinations = session.sql("SELECT * FROM country_vaccinations")
    val country_vaccinations_by_manufacturer = session.sql("SELECT * FROM country_vaccinations_by_manufacturer")

    val country_date_window_spec = Window.partitionBy("location").orderBy(col("date").desc)
    val latest_country_vaccinations_by_manufacturer =
      country_vaccinations_by_manufacturer
        .withColumn("row_num", row_number().over(country_date_window_spec)).filter(col("row_num") === 1).drop("row_num")

    val total_vaccinations_by_manufacturer =
      latest_country_vaccinations_by_manufacturer
        .groupBy(country_vaccinations_by_manufacturer.col("vaccine"))
        .sum("total_vaccinations")
        .select(
          col("vaccine"),
          col("sum(total_vaccinations)").alias("total")
        ).sort(col("total").desc)
    total_vaccinations_by_manufacturer.show()
    total_vaccinations_by_manufacturer.write.mode(SaveMode.Overwrite).saveAsTable("total_vaccinations_by_manufacturer")

  }
}
