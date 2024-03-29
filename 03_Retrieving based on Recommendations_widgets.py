# Databricks notebook source
# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC %pip install solara
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Detailed information and step by step instructions on setting up a MosaicML Inference Endpoint for LlaMA-70B-Chat as a route in MLFlow AI Gateway is provided at https://www.databricks.com/blog/using-ai-gateway-llama2-rag-apps. 
# MAGIC
# MAGIC MLFlow AI Gateway documentation is given here: https://mlflow.org/docs/latest/gateway/index.html
# MAGIC
# MAGIC MosaicML Starter Tier details and instructions are provided here: https://www.mosaicml.com/blog/llama2-inference

# COMMAND ----------

import re
import os
import requests
import numpy as np
import pandas as pd
import json
import random
from ipywidgets import interact, widgets
from IPython.display import display
import asyncio
from databricks.vector_search.client import VectorSearchClient
from typing import List, Dict, Any, Union

# COMMAND ----------

client = VectorSearchClient()

# COMMAND ----------

# MAGIC %md
# MAGIC As discussed earlier LLaMA Chat models have been instruction finetuned on data with a specific formatas seen below:
# MAGIC
# MAGIC ```
# MAGIC [INST] <<SYS>>
# MAGIC {{ system_prompt }}
# MAGIC <</SYS>>
# MAGIC
# MAGIC {{ user_message }} [/INST]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC The prompt generation function for this recommendation task, created based on some prompt engineering while conforming to the above format, is as follows

# COMMAND ----------

def format_string(system_prompt: str, items: List[str])-> str:
    formatted_string = f"[INST] <<SYS>>\n{system_prompt}\n<</SYS>>\n"
    
    items_purchased = ', '.join(items[:-1]) + ', and ' + items[-1] if len(items) > 1 else items[0]
    
    next_item_question = f"A user bought {items_purchased} in that order. What single item would he/she be likely to purchase next?"
    
    formatted_string += f"{next_item_question} Express as a python list 'next_item' with an inline description[/INST]"
    
    return formatted_string

# Example usage:
system_prompt = "You are an AI assistant functioning as a recommendation system for an ecommerce website. Be specific and limit your answers to the requested format."
#items = ['scarf', 'beanie', 'ear muffs', 'long socks']
items = ['vaccuum cleaner', 'electric broom', 'smart trashcan']
formatted_text = format_string(system_prompt, items)
print(formatted_text)

# COMMAND ----------

# Query the completions route using the mlflow client
# URL and endpoint
url = 'https://<your-workspace>/serving-endpoints/databricks-llama-2-70b-chat/invocations'
token = "your-token"
def llm_predict(formatted_text, workspace_url, token):
    url = workspace_url

    # Headers for the request
    headers = {
        'Content-Type': 'application/json',
    }

    # Prepare the data following the structure provided in the example
    data = {
        "messages": [
            {
                "role": "user",
                "content": formatted_text
            }
        ],
        "max_tokens": 128
    }

    # Get the authentication token from environment variables
    # Replace the following with your actual token or retrieve from an environment variable
    auth_token = token

    # Make the POST request
    response = requests.post(url, headers=headers, json=data, auth=('token', auth_token))

    # Print the response
    return json.loads(response.text)['choices'][0]['message']["content"]

# COMMAND ----------

next_item = llm_predict(formatted_text, url, token)

# COMMAND ----------

next_item

# COMMAND ----------

df = pd.DataFrame({'text': [next_item]})
display(df)

# COMMAND ----------

type(client)

# COMMAND ----------

def vector_search(client: 'databricks.vector_search.client.VectorSearchClient', num_items: int, next_item: str)->List[str]:
  results = client.get_index(index_name="llm_recommender.movie_data.reddot_product_index", endpoint_name="llm4rec_endpoint").similarity_search(
  query_text=next_item,
  columns=["id", "text"],
  num_results=3)
  return [item[-2] for item in results['result']['data_array']]

# COMMAND ----------

item = 'vacuum cleaner'
vector_search(client, 3,next_item)

# COMMAND ----------

# MAGIC %md
# MAGIC The entire LLM based recommendation task can can be neatly wrapped into a function as follows

# COMMAND ----------

def LLM4rec(system_prompt: str, items: List[str], client: 'databricks.vector_search.client.VectorSearchClient', num_recs: int)-> Dict[str, List[str]]:
  #This random item can be anything, to ensure there's something to recommend given an absolute cold start
  #i.e. the user has not purchased anything yet/ visited any item's webpage before
  starter_items =  [ "Smartphone", "Laptop", "Headphones", "T-shirt", "Jeans", "Running Shoes", "Blender", "Novel", "Board Game", "Wristwatch" ]
  if len(items)==0:
    items.append(random.sample(starter_items, 1))
  prompt = format_string(system_prompt, items)
  next_item = llm_predict(formatted_text, url, token)
  recommendations = vector_search(client, num_recs,next_item)
  return recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC Testing the above function 

# COMMAND ----------

user_purchase_histories = {'Ben':['hammer', 'nails', 'saw', 'super glue'],
                           'Jess': ['pen', 'pencil', 'eraser'],
                           'Kumar': ['bicycle'],
                           'Jose': ['car seat', 'diapers', 'tissue'],
                           'noob': []}

# COMMAND ----------

df = pd.DataFrame(list(user_purchase_histories.items()), columns=['User', 'Purchases'])

display(df)

# COMMAND ----------

system_prompt = "You are an AI assistant functioning as a recommendation system for an ecommerce website. Be specific and limit your answers to the requested format."

# COMMAND ----------

LLM4rec(system_prompt, user_purchase_histories['Ben'], client, 3)

# COMMAND ----------

# MAGIC %md
# MAGIC Great!! All these recommendations make sense. This is only meant to be an example, but there is an entire research field actively pursuing this idea

# COMMAND ----------

# Create a dropdown widget with the names from the dictionary keys
name_dropdown = widgets.Dropdown(
    options=user_purchase_histories.keys(),
    description='Select a name:',
)
name_dropdown.layout.width = '300px'

# Create a dropdown widget with the number of recommendations desired
numrecs_dropdown = widgets.Dropdown(
    options=[1,2,3],
    description='Select the number of recommendations:',
)
# Adjust the style to ensure the full description is shown
name_dropdown.style.description_width = 'initial'
# Adjust the style to ensure the full description is shown
numrecs_dropdown.style.description_width = 'initial'

numrecs_dropdown.layout.width = '300px'

# Create a button widget to trigger the recommendation
recommend_button = widgets.Button(description='Recommend')

# Create an output widget to display the recommendations
output = widgets.Output()

# COMMAND ----------

# Define a function to handle button click
def recommend_button_click(b):
    selected_name = name_dropdown.value
    number = numrecs_dropdown.value
    recommendations = LLM4rec(system_prompt, user_purchase_histories[selected_name], client, number)
    
    # Display the recommendations
    with output:
        output.clear_output()
        print("Recommendations:")
        print(" ")
        for recommendation in recommendations:
            print(recommendation)
            print("  ")
      

# COMMAND ----------

display(df)

# COMMAND ----------

# Connect the button click event to the function
recommend_button.on_click(lambda b: recommend_button_click(b))

# Display the widgets
display(name_dropdown, numrecs_dropdown,recommend_button, output)
