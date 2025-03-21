from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, min, max, stddev, corr

# Initialize a Spark session
spark = SparkSession.builder.appName("Car Price Dataset Analysis").getOrCreate()

# Load the dataset
file_path = r'C:\Users\Dell\OneDrive\Desktop\CODTECH INTERNSHIP TASK\car_price_dataset.csv'  # Use raw string for Windows paths
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Display schema to check column names and types
df.printSchema()

# Display a sample of the dataset
df.show(20, truncate=False)

# Get total number of records
record_count = df.count()
print(f"Total number of records: {record_count}")

# Group by 'Model' and count occurrences
if "Model" in df.columns:
    df.groupBy("Model").count().orderBy(col("count").desc()).show(10)

# Summary statistics for numerical columns (replace 'Price' with actual numerical columns)
numeric_cols = [col_name for col_name, dtype in df.dtypes if dtype in ("int", "double")]
df.select([mean(col(c)).alias(f"mean_{c}") for c in numeric_cols]).show()
df.select([stddev(col(c)).alias(f"stddev_{c}") for c in numeric_cols]).show()
df.select([min(col(c)).alias(f"min_{c}") for c in numeric_cols]).show()
df.select([max(col(c)).alias(f"max_{c}") for c in numeric_cols]).show()

# Correlation between 'Price' and other numerical features (replace 'Price' with actual column name)
if "Price" in numeric_cols:
    for col_name in numeric_cols:
        if col_name != "Price":
            correlation = df.stat.corr("Price", col_name)
            print(f"Correlation between Price and {col_name}: {correlation:.2f}")

# Filter cars with price > 10,000 and display top 20
if "Price" in df.columns:
    filtered_data = df.filter(col("Price") > 10000)
    filtered_data.show(20)

    # Save filtered data as a single CSV file
    output_path = r'C:/Users/Dell/OneDrive/Desktop/filtered_car_prices'
    filtered_data.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# Stop the Spark session
spark.stop()
