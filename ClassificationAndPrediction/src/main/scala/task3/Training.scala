package task3

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor

object Training {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger


    val spark  = SparkSession.builder().appName("Twitter Follower")
        .config("spark.master", "local")
        .getOrCreate()

    val dataset = spark.read.format("csv").option("header", "true").load(args(0))

    //TypeCaste all Values in the dataset as numeric type from string type
    val castedDF = dataset.columns.foldLeft(dataset)((current, c) => current.withColumn(c, col(c).cast("float")))

    //Drop non numeric columns
    var finalDf = castedDF.drop("key").drop("pickup_datetime")

    //Defining columns that will be used as features
    val featureCols = Array("pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count")

    //Drop all the null value rows form the dataset
    finalDf=finalDf.na.drop()

    finalDf = finalDf.filter(col("fare_amount") > 2.5)
    finalDf = finalDf.filter(col("passenger_count") =!= 0)
    finalDf = finalDf.filter(col("pickup_longitude") =!= 0)
    finalDf = finalDf.filter(col("pickup_latitude") =!= 0)
    finalDf = finalDf.filter(col("dropoff_longitude") =!= 0)
    finalDf = finalDf.filter(col("dropoff_latitude") =!= 0)

    //Vectorize the features for training the model
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(finalDf)
    df2.show()

    val Array(trainingData, testData) = df2.randomSplit(Array(0.8, 0.2))

    //Fitting Random Forest model on the training data

    val rf = new RandomForestRegressor()
        .setLabelCol("fare_amount").setMaxDepth(30)
        .setFeaturesCol("features").fit(trainingData)

    //Predicting values
    val prediction = rf.transform(testData)

    prediction.show()

    val evaluator = new RegressionEvaluator()
        .setLabelCol("fare_amount")
        .setPredictionCol("fare_amount")
        .setMetricName("rmse")

    val rmse = evaluator.evaluate(prediction)

    logger.info("Root Mean Squared Error (RMSE) on test data = " + rmse)

  }

}
