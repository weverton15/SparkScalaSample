package sample

import utils.Utils
import org.apache.spark.sql.SparkSession

object SparkScalaSampleApplication {
  def main(arg: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.master("local").appName("Scala Spark Example").getOrCreate()
    val dfUserReviews = sparkSession.read.option("inferSchema", true).option("header", true).csv("googleplaystore_user_reviews.csv")
    val dfMobileApp = sparkSession.read.option("inferSchema", true).option("header", true).csv("googleplaystore.csv")

    val df_1 = Utils.generateAveragePolatiry(dfUserReviews)
    df_1.show()

    val df_2 = Utils.generateBestApp(dfMobileApp, "best_apps.csv", "§")
    df_2.show()

    val df_3 = Utils.generateUniqueAppValue(dfMobileApp)
    df_3.show(truncate = false)

    val df = Utils.generateCleaned(df_1, df_3, "googleplaystore_cleaned", "gzip")
    df.show(truncate = false)

    val df_4 = Utils.generateMetrics(df, "googleplaystore_metrics", "gzip")
    df_4.show(truncate = false)
  }
}
