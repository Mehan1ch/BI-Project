from dotenv import load_dotenv
import os

load_dotenv()

#Import kaggle credentials from .env file
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY= os.getenv("KAGGLE_KEY")

import kaggle

kaggle.api.authenticate()


# Path to download the dataset
download_path = "../Data"

# Create the directory if it does not exist
if not os.path.exists(download_path):
    os.makedirs(download_path)

# Download latest version

kaggle.api.dataset_download_files('schmoyote/coffee-reviews-dataset', path=download_path+"/coffe-reviews-dataset/", unzip=True)
kaggle.api.dataset_download_files("ahmedmohamedibrahim1/coffee-shop-sales-dataset", path=download_path+"/coffee-shop-sales-dataset/", unzip=True)
kaggle.api.dataset_download_files("fatihb/coffee-quality-data-cqi", path=download_path+"/coffee-quality-data-cqi/", unzip=True)
kaggle.api.dataset_download_files("ihelon/coffee-sales", path=download_path+"/coffee-sales/", unzip=True)
kaggle.api.dataset_download_files("parasrupani/coffee-distribution-across-94-counties", path=download_path+"/coffee-distribution-across-94-counties/", unzip=True)

print("Dataset downloaded")
