package ru.sbrf.ca.drpa

import org.apache.spark.sql.SparkSession

/***
 * Слой предназначен для расчета и сохранения промежуточный витрин
 */
object BusinessLayer {

  def run()(implicit session: SparkSession): Unit = {
    val happiness_df = session.sql("SELECT * FROM happiness_2019")
    val country_vaccinations = session.sql("SELECT * FROM country_vaccinations")
    val country_vaccinations_by_manufacturer = session.sql("SELECT * FROM country_vaccinations_by_manufacturer")
  }
}
