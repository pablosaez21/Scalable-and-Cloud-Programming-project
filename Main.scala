import org.apache.spark.sql.{SparkSession, functions => F}

object OrderProductsCoPurchaseAnalysis {

  def main(args: Array[String]): Unit = {
    // Define paths directly in code
    val gcsInputPath = "gs://bucket-scala-pablo/order_products.csv" // Path to CSV file in GCS
    val gcsOutputPath = "gs://bucket-scala-pablo/output/"           // Output path in GCS

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("OrderProductsCoPurchaseAnalysis")
      .getOrCreate()

    // Read CSV file from GCS
    val data = spark.read
      .option("header", "false") // File has no headers
      .option("inferSchema", "true") // Infer schema automatically
      .csv(gcsInputPath)
      .toDF("order_id", "product_id") // Rename columns

    // Display loaded data (for debugging)
    data.show()

    // Generate product pairs for each order
    val productPairs = data
      .join(data, "order_id") // Combine products within each order
      .filter(F.col("product_id") < F.col("product_id_2")) // Avoid reverse pairs
      .select(F.col("product_id").alias("x"), F.col("product_id_2").alias("y"))

    // Count occurrences of each product pair
    val coPurchaseCounts = productPairs
      .groupBy("x", "y")
      .agg(F.count("*").alias("n"))

    // Save results as CSV in GCS
    coPurchaseCounts.write
      .option("header", "true") // Include headers
      .mode("overwrite") // Overwrite if exists
      .csv(s"$gcsOutputPath/co_purchase_analysis")

    // Stop Spark
    spark.stop()
  }
}
