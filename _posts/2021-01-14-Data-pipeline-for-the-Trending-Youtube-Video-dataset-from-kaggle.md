---
layout: post
title: "Data pipeline for the Trending YouTube Video dataset from kaggle"
date: 2021-01-14
category: databricks
published: True
---

The dataset for the Indian YouTube trending page is not proper to perform insightful analysis. So, I created a ETL pipeline to clean-up the dataset and make it easier to perform analysis. This post is all about how I created this pipeline using PySpark on the Databricks platform.



<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/main/assets/images/youtube_banner.jpg?raw=true" alt="youtube_banner">
</html>

For the uninitiated, 

> YouTube (the world-famous video sharing website) maintains a list of the [top trending videos](https://www.youtube.com/feed/trending) on the platform. [According to Variety magazine](http://variety.com/2017/digital/news/youtube-2017-top-trending-videos-music-videos-1202631416/), “To determine the year’s top-trending videos, YouTube uses a combination of factors including measuring users interactions (number of views, shares, comments and likes). Note that they’re not the most-viewed videos overall for the calendar year”. Top performers on the YouTube trending list are music videos (such as the famously virile “Gangam Style”), celebrity and/or reality TV performances, and the random dude-with-a-camera viral videos that YouTube is well-known for.
>
> This dataset is a daily record of the top trending YouTube videos.
>
> [*-kaggle*](https://www.kaggle.com/datasnaek/youtube-new)

There is data on 10 countries but our dataset of interest is for India. 

**You can view my notebook [here](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1294872003561177/2970472917297418/7024709780737754/latest.html) or by visiting my [GitHub repo](https://github.com/TakreemAkhter/Data-pipeline-for-Youtube-trending-videos-dataset).**

## <u>Problems with the dataset</u>

- The file provided is `csv`, hence there is no defined schema. This makes it difficult to extract any information using built-in functions specific to certain datatypes right-away. There are columns which consist of date and time. They can be easily analyzed if the schema is corrected/defined. 
- There are rows that stretch multiple lines. They maybe interpreted as separate rows by spark if not corrected and can lead to incorrect row/column values.
- Another disadvantage of a `csv` file is that they are slow to parse with Spark. They are not able to take advantage of Spark's *predicate pushdown*, which help with performance improvements.
- There are rows of data present for video which have been removed by YouTube or have some kind of error. Such data can influence insights and can result to *false positives*.

## <u>The Solution</u>

The answer is simple, to create a ingestion data pipeline that extracts, transforms and loads the data into a more robust format.

I used PySpark on the Databricks platform and wrote the cleaned data-frame to *parquet*.

## <u>The Procedure</u>

- [x] To ingest the data into databricks, I used the **kaggle CLI**. I installed it in the environment but to use it, I needed to export my token key and username which would inherently expose my credentials. To conceal it from the viewers, I used [widgets](https://docs.databricks.com/notebooks/widgets.html#widgets) that allow us to add parameters to a notebook. This way, anybody using my notebook can run it by entering their own token and username.  You can refer the [kaggle API documentation](https://github.com/Kaggle/kaggle-api) to learn about the various commands and how to install it.

- [x] Then I defined the schema according to the nature of the columns and read the file into a dataframe. 

  The date format for the `trending_date` column is `yy.dd.MM` and the timestamp format for the `publish_time` column is `yyyy-MM-dd'T'HH:mm:ss.SSSZ`. This cannot be specified in the schema. To capture this format, I specified the [*dateFormat* and the *timestampFormat* options](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=collect#pyspark.sql.DataFrameReader) while reading the file to a dataframe. To know more about these options click on the link. 

  I specified the *multiLine* options as well to convey to Spark that some rows stretch multiple lines and to not parse such records as separate rows.

- [x] After importing the data, I performed the relevant transformations.

  I removed the rows which had `video_error_or_removed` column set to `True`. Value set to true for this column points out that the video has some technical error or it has been removed and hence, cannot be played.

  I even removed the rows which had `video_id` column set to `#NAME?`. Such values also indicate that the video has been removed by YouTube and hence they have not been assigned a id.

  Such records may introduce biases to our analyses and can result to false positives. I have done my best to find such noise in the data and remove them. 

- [x] The string datatype for the `tags` column is not helpful when trying to analyze tags from all the videos. So I added another column which has all of the video's tags in the form of a list. This helps in better analysis.

- [x] I wrote the dataframe to parquet and stored it in <u>dbfs:/FileStore</u> folder. To know more about FileStore, visit [this link](https://docs.databricks.com/data/filestore.html#filestore). The parquet file can be downloaded from [this link](https://community.cloud.databricks.com/files/INvideos_cleaned.parquet/part-00000-tid-3075629440830729008-33288d04-1503-4208-9764-75b3d5e30c83-14-1-c000.snappy.parquet?o=1294872003561177). 