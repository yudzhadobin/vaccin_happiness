package ru.sbrf.ca.drpa

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{SaveMode, SparkSession}

object MartLayer {

  def run()(implicit session: SparkSession): Unit = {
    val main = session.sql("SELECT * FROM happiness_2019 h join country_vaccinations cv on h.Country_or_region = cv.xcountry")

    val vaccines_by_rank = main.select(col("people_fully_vaccinated_per_hundred"), col("Overall_rank")).sort(col("Overall_rank"))
    val vaccines_by_gdp = main.select(col("people_fully_vaccinated_per_hundred"), col("GDP_per_capita")).sort(col("GDP_per_capita"))
    val vaccines_by_healthy = main.select(col("people_fully_vaccinated_per_hundred"), col("Healthy_life_expectancy")).sort(col("Healthy_life_expectancy"))
    val vaccines_by_freedom = main.select(col("people_fully_vaccinated_per_hundred"), col("Freedom_to_make_life_choices")).sort(col("Freedom_to_make_life_choices"))
    val vaccines_by_corruption = main.select(col("people_fully_vaccinated_per_hundred"), col("Perceptions_of_corruption")).sort(col("Perceptions_of_corruption"))

    val error_vaccines_country = session.sql("SELECT distinct Country_or_region FROM happiness_2019 where Country_or_region not in (select xcountry from country_vaccinations)")
    val error_happiness_country = session.sql("SELECT distinct xcountry FROM country_vaccinations where xcountry not in (select Country_or_region from happiness_2019)")

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
    total_vaccinations_by_manufacturer.write.mode(SaveMode.Overwrite).saveAsTable("total_vaccinations_by_manufacturer")
  }

}
