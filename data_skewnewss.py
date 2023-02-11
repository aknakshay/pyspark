# pyspark

import pyspark.sql.functions as F
import pandas as pd 

def total_partitions(data):
    """
    Returns the total number of partitions in the dataframe
    """
    return data.rdd.getNumPartitions()

def find_skewness(data):
    """
    Finds count of rows per partition
    """
    return data.groupBy(F.spark_partition_id()).count()

def plot_skewness(data):
    """
    Converts dataframe to pandas and plot a histogram
    """
    plot = data.toPandas().hist(column="count")
    plot.flatten()[0].set_xlabel("count of rows in a partition")
    plot.flatten()[0].set_ylabel("number of partitions having this count")
    plot.flatten()[0].set_title("data skewness")


def get_report(data):
    """
    Tells about total number of partitions
    Plots the skewness
    """
    print("Total Partitions " + str(data))
    skewness_df = find_skewness(data)
    plot_skewness(skewness_df)

# How to use?
# get_report(data)