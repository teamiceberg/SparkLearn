import os
import sys
from pyspark.sql import SparkSession
import pandas as pd

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: mnmcount<datafile> <state>", file=sys.stderr)
        sys.exit(-1)

# read in command line args
mnm_file = sys.argv[1]
mnm_state = sys.argv[2].strip().upper()

# we use pandas dataframe here for error checking
# Spark does this with df.selectExpr('sql query'). Small csv file - so pandas is ok
# for large csv files, Spark read ins are the way to go. 

state_abbs_df = pd.read_csv('./us-states-territories.csv', header=0)
print(state_abbs_df.info())
if mnm_state not in ['All', 'all', 'ALL']\
            and  mnm_state not in state_abbs_df['Abbreviation'].str.strip().values : 
    print("usage: enter valid state abbreviation e.g. California = CA. For all states use 'All' ", file=sys.stderr)
    sys.exit(-1)

''' Build a sparksession '''

spark = (SparkSession
        .builder
        .appName("MnMCount")
        .getOrCreate())


# read the file into a Spark dataframe
mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))

# graph 1: color count of M&Ms for each state
#this is the computational graph. No computation happpens yet.
color_count_mnm_df = (mnm_df
                        .select("State", "Color", "Count")
                        .groupBy("State", "Color")
                        .sum("Count")
                        .orderBy("sum(Count)", ascending=False)
                        )

#graph 2: see by a specific state
#setup computation graph
state_color_count_mnm_df = (mnm_df
                        .select("State", "Color", "Count")
                        .where(mnm_df.State == mnm_state)
                        .groupBy("State", "Color")
                        .sum("Count")
                        .orderBy("sum(Count)", ascending=False)
                        )

# execute graphs

if mnm_state in  ['All', 'all', 'ALL']:
    color_count_mnm_df.show(n=60, truncate=False)
    print ("total rows={}". format(color_count_mnm_df.count()))

else: 
    state_color_count_mnm_df.show(n=10, truncate=False)
    print ("total rows={}". format(state_color_count_mnm_df.count()))

#terminate Spark session
spark.stop()