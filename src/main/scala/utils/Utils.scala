package utils

import org.apache.spark.sql.functions.{asc, avg, col, desc, regexp_replace}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, DatasetHolder, SaveMode}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Utils {

  def generateAveragePolatiry(dataFrame: DataFrame): DataFrame = {
    val dfAveragePolatiry = dataFrame
      .withColumn("Sentiment_Polarity", regexp_replace(col("Sentiment_Polarity"), "[nan]", "0"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))
      .withColumn("Sentiment_Subjectivity", regexp_replace(col("Sentiment_Subjectivity"), "[nan]", "0"))
      .withColumn("Sentiment_Subjectivity", col("Sentiment_Subjectivity").cast(DoubleType))
      .groupBy("App").agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))
      .orderBy("App")
      .select("App", "Average_Sentiment_Polarity")

    dfAveragePolatiry
  }

  def generateBestApp(dataFrame: DataFrame, csvName: String, delimiter: String): DataFrame = {
    val dfBestApp = dataFrame
      .withColumn("Rating", regexp_replace(col("Rating"), "[NaN]", "0"))
      .withColumn("Rating", col("Rating").cast(DoubleType))
      .where(col("Rating") >= 4.0)
      .orderBy(desc("Rating"), asc("App"))
      .select("App", "Rating")

    dfBestApp.repartition(1).write.mode(SaveMode.Overwrite).format("csv")
      .option("header", true).option("delimiter", delimiter).csv(csvName)

    dfBestApp
  }

  def generateUniqueAppValue(dataFrame: DataFrame): DataFrame = {
    val df_3_temp = dataFrame.withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumn("Rating", regexp_replace(col("Rating"), "NaN", "null"))
      .withColumn("Reviews", col("Reviews").cast(LongType))
      .withColumn("Price", regexp_replace(col("Price"), "[$]", ""))
      .withColumn("Price", col("Price").cast(DoubleType))
      .withColumn("last_updated", to_date(trim(col("last_updated")), "MMMM dd, yyyy"))

    val dfJoin = df_3_temp
      .groupBy("App")
      .agg(
        collect_set(col("Category")).as("Categories"),
        max(col("Reviews")).as("Reviews")
      )
      .select("App", "Categories", "Reviews")

    val df_3 = df_3_temp
      .join(
        dfJoin,
        df_3_temp.col("App") === dfJoin.col("App") && df_3_temp.col("Reviews") === dfJoin.col("Reviews"),
        "right"
      )
      .withColumn("Size",
        when(
          !trim(df_3_temp("Size")).startsWith("Varies with device") && trim(df_3_temp("Size")).contains("K"),
          regexp_replace(df_3_temp("Size"), "[a-zA-Z]*", "").cast(DoubleType) * 1024
        )
          .when(
            !trim(df_3_temp("Size")).startsWith("Varies with device") && trim(df_3_temp("Size")).contains("G"),
            regexp_replace(df_3_temp("Size"), "[a-zA-Z]*", "").cast(DoubleType) / 1024
          )
          .when(
            !trim(df_3_temp("Size")).startsWith("Varies with device") && trim(df_3_temp("Size")).contains("M"),
            regexp_replace(df_3_temp("Size"), "[a-zA-Z]*", "").cast(DoubleType)
          )
          .otherwise(0)
      )
      .withColumn("Price", when(df_3_temp("Price") !== 0, df_3_temp("Price") * 0.9).otherwise(df_3_temp("Price")))
      .withColumn("Genres", split(col("Genres"), ";").as("Genres"))
      .select(
        dfJoin.col("App"),
        dfJoin.col("Categories"),
        df_3_temp.col("Rating"),
        dfJoin.col("Reviews"),
        col("Size"),
        df_3_temp.col("Installs"),
        df_3_temp.col("Type"),
        col("Price"),
        df_3_temp.col("Content_Rating"),
        col("Genres"),
        df_3_temp.col("Last_Updated"),
        df_3_temp.col("Current_Version"),
        df_3_temp.col("Minimum_Android_Version")
      )

    df_3
  }

  def generateCleaned(dataFrameAveragePolatiry: DataFrame, dataFrameUniqueValue: DataFrame, parquetName: String, compression: String): DataFrame = {
    val df = dataFrameUniqueValue.join(
      dataFrameAveragePolatiry,
      dataFrameUniqueValue.col("App") === dataFrameAveragePolatiry.col("App"),
      "left"
    )
      .select(
        dataFrameUniqueValue.col("App"),
        dataFrameUniqueValue.col("Categories"),
        dataFrameUniqueValue.col("Rating"),
        dataFrameUniqueValue.col("Reviews"),
        dataFrameUniqueValue.col("Size"),
        dataFrameUniqueValue.col("Installs"),
        dataFrameUniqueValue.col("Type"),
        dataFrameUniqueValue.col("Price"),
        dataFrameUniqueValue.col("Content_Rating"),
        dataFrameUniqueValue.col("Genres"),
        dataFrameUniqueValue.col("Last_Updated"),
        dataFrameUniqueValue.col("Current_Version"),
        dataFrameUniqueValue.col("Minimum_Android_Version"),
        dataFrameAveragePolatiry.col("Average_Sentiment_Polarity")
      )

    df.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", true).option("compression", compression).parquet(parquetName)

    df
  }

  def generateMetrics(dataFrame: DataFrame, parquetName: String, compression: String): DataFrame = {
    val dfExplode = dataFrame
      .withColumn("Genre", explode(dataFrame.col("Genres")))
      .withColumn("Rating", regexp_replace(col("Rating"), "[NaN]", "0"))
      .withColumn("Rating", col("Rating").cast(DoubleType))
      .withColumn("Average_Sentiment_Polarity", regexp_replace(col("Average_Sentiment_Polarity"), "[null]", "0"))
      .withColumn("Average_Sentiment_Polarity", col("Average_Sentiment_Polarity").cast(DoubleType))

    val df_4 = dfExplode
      .groupBy("Genre")
      .agg(
        count("App").as("Count"),
        avg("Rating").as("Average_Rating"),
        avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )
      .orderBy("Genre")

    df_4.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", true).option("compression", compression).parquet(parquetName)

    df_4
  }
}
