# Ploomber API
# %% tags=["parameters"]
upstream = None
product = None
data_path = None

# %%
from dotenv import load_dotenv
import os

load_dotenv()

#Import kaggle credentials from .env file
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY= os.getenv("KAGGLE_KEY")

import kaggle

kaggle.api.authenticate()

# Create the directory if it does not exist
if not os.path.exists(data_path):
    os.makedirs(data_path)

# Download
kaggle.api.dataset_download_files("ihelon/coffee-sales", path=data_path+"/coffee-sales/", unzip=True)
