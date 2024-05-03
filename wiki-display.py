from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from pyspark.sql.functions import min, max, expr, current_timestamp, col

# Initialize Spark Session
spark = SparkSession.builder.appName("WikiChangesAnalysis").getOrCreate()

def update(frame):
    plt.cla()  # Clear the current plot
    
    # Calculate the time 5 minutes ago from the current time
    five_minutes_ago = current_timestamp() - expr("INTERVAL 5 MINUTES")
    
    # Read the parquet files and filter for records within the past 5 minutes
    df = spark.read.parquet("output").filter(col("change_timestamp") >= five_minutes_ago)

    # Group by title, count occurrences, and sort by count in descending order
    title_counts = df.groupBy("title").count().orderBy("count", ascending=False)

    # Get minimum and maximum change_timestamp within the filtered time range
    time_range = df.agg(min("change_timestamp").alias("min_time"), max("change_timestamp").alias("max_time")).collect()
    min_time = time_range[0]['min_time']
    max_time = time_range[0]['max_time']

    # Convert to Pandas DataFrame for plotting
    title_counts_pd = title_counts.toPandas()

    # Plotting the data
    plt.bar(title_counts_pd['title'][:10], title_counts_pd['count'][:10], color='blue')  # Displaying top 10 titles
    plt.xlabel('Title')
    plt.ylabel('Count')
    plt.title(f'Top 10 Wiki Title Changes by Count\nDate Range: {min_time} to {max_time}')
    plt.xticks(rotation=45)
    plt.tight_layout()

# Set up plot to call `update()` method every 2000 milliseconds (2 seconds)
ani = FuncAnimation(plt.gcf(), update, interval=2000)

plt.show()

# Stop the Spark session
spark.stop()
