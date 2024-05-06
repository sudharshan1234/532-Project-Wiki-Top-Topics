from flask import Flask, render_template_string
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, expr, col

app = Flask(__name__)

# Initialize Spark Session
spark = SparkSession.builder.appName("WikiChangesAnalysis").getOrCreate()

@app.route('/')
def show_dataframe():

    # Calculate the time 30 minutes ago from the current time
    thirty_minutes_ago = current_timestamp() - expr("INTERVAL 30 MINUTES")

    # Read the parquet files and filter for records within the past 30 minutes
    df = spark.read.parquet("output").filter(col("change_timestamp") >= thirty_minutes_ago)

    # Group by title, count occurrences, and sort by count in descending order
    # Group by title, count occurrences, and sort by count in descending order
    title_counts = df.groupBy("title").count().orderBy(col("count").desc())

    # Convert the DataFrame to Pandas, keep only top 20 entries
    title_counts_pd = title_counts.toPandas().head(20)

    # Convert the DataFrame to HTML table
    html = title_counts_pd.to_html(index=True, classes='table table-striped')

    # HTML template rendering the DataFrame as a table
    return render_template_string("""
        <!doctype html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport"
                  content="width=device-width, user-scalable=no, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0">
            <meta http-equiv="X-UA-Compatible" content="ie=edge">
            <title>Data Table</title>
            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css">
            <script>
              setTimeout(function() {
                location.reload();
              }, 2000); // Reload the page every 2 seconds (2000 milliseconds)
            </script>
        </head>
        <body>
            <div class="container">
                <h1 class="mt-5">Top 20 topics in past 30 minutes (Updated every 2 seconds)</h1>
                {{ html|safe }}
            </div>
        </body>
        </html>""", html=html)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)