# %% tags=["parameters"]
upstream = ['extract']
product = None
extract_path = None
transform_path = None

# %%
import pandas as pd

def remove_unnecessary_columns(columns_to_remove):
    df = pd.read_csv(file_path)
    df.drop(columns=columns_to_remove, inplace=True)
    return df

def save_to_csv(df, output_path):
    df.to_csv(output_path, index=False)


def load_csv(file_path):
    return pd.read_csv(file_path)

columns_to_remove = ['Exports', 'Imports']
file_path = extract_path + '/coffee-distribution-across-94-counties/psd_coffee.csv'

df = load_csv(file_path)
remove_unnecessary_columns(columns_to_remove, product['data'])
save_to_csv(df, product['data'])
