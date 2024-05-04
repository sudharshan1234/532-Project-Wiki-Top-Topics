from flask import Flask, send_file
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import min, max, expr, current_timestamp, col
import io

app = Flask(__name__)

# Initialize Spark Session
spark = SparkSession.builder.appName("WikiChangesAnalysis").getOrCreate()

@app.route('/plot.png')
def plot_png():
    plt.cla()  # Clear any existing plot

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

    # Save plot to a bytes buffer
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)

    return send_file(buffer, mimetype='image/png', as_attachment=False)

@app.route('/')
def index():
    return '<img src="/plot.png" alt="Plot Image"/>'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)