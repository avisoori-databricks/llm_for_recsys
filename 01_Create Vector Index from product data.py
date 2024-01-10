# Databricks notebook source
# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datasets import load_dataset , Dataset, concatenate_datasets 
import pandas as pd

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
client = VectorSearchClient()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE A CATALOG, SCHEMA AND VOLUME TO STORE DATA NEEDED FOR THIS. IN PRACTICE, YOU COULD USE AN EXISTING VOLUME
# MAGIC CREATE CATALOG IF NOT EXISTS llm_recommender;
# MAGIC USE CATALOG llm_recommender;
# MAGIC CREATE DATABASE IF NOT EXISTS movie_data;
# MAGIC USE DATABASE movie_data;
# MAGIC CREATE VOLUME IF NOT EXISTS movie_lens;

# COMMAND ----------

prod_ds = load_dataset("xiyuez/red-dot-design-award-product-description")
prod_df = pd.DataFrame(prod_ds['train'])
display(prod_df)

# COMMAND ----------

prod_df['id'] = prod_df.reset_index().index
df = prod_df[['id', 'text']]
display(df)

# COMMAND ----------

spark.createDataFrame(df).write.saveAsTable('product_descriptions')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE product_descriptions SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

client.create_endpoint(
    name="llm4rec_endpoint",
    endpoint_type="STANDARD" #PERFORMANCE_OPTIMIZED, STORAGE_OPTIMIZED
)

# COMMAND ----------

client.list_endpoints()

# COMMAND ----------

client.create_delta_sync_index(
  endpoint_name="llm4rec_endpoint",
  source_table_name="llm_recommender.movie_data.product_descriptions",
  index_name="llm_recommender.movie_data.reddot_product_index",
  pipeline_type='TRIGGERED',
  primary_key="id",
  embedding_source_column="text",
  embedding_model_endpoint_name="all-MiniLM-L6-v2-avi"
)

# COMMAND ----------

results = client.get_index(index_name="llm_recommender.movie_data.reddot_product_index", endpoint_name="llm4rec_endpoint").similarity_search(
  query_text="smart vacuum cleaner",
  columns=["id", "text"],
  num_results=3)

# COMMAND ----------

results

# COMMAND ----------


