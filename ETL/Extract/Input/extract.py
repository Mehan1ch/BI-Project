# %% tags=["parameters"]
upstream = None
product = None
extract_path = None

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
if not os.path.exists(extract_path):
    os.makedirs(extract_path)

# Download latest version
kaggle.api.dataset_download_files("parasrupani/coffee-distribution-across-94-counties", path=extract_path + "/distribution/", unzip=True)
kaggle.api.dataset_download_files("fatihb/coffee-quality-data-cqi", path=extract_path + "/quality/", unzip=True)
kaggle.api.dataset_download_files('schmoyote/coffee-reviews-dataset', path=extract_path + "/reviews/", unzip=True)
kaggle.api.dataset_download_files("ihelon/coffee-sales", path=extract_path + "/sales/", unzip=True)
kaggle.api.dataset_download_files("ahmedmohamedibrahim1/coffee-shop-sales-dataset", path=extract_path + "/shop-sales/", unzip=True)
