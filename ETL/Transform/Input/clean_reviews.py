# %% tags=["parameters"]
import pandas

upstream = ['extract']
product = None
extract_path = None
transform_path = None

# %%
import pandas as pd

def remove_unnecessary_columns(dataframe: pandas.DataFrame, columns: [str]) -> pandas.DataFrame:
    dataframe.drop(columns=columns, inplace=True)
    return dataframe

def save_to_csv(df: pandas.DataFrame, output_path: str) -> None:
    df.to_csv(output_path, index=False)

def load_csv(input_path: str) -> pandas.DataFrame:
    return pd.read_csv(input_path)

columns_to_remove = ['origin_1','review_date','desc_1','desc_2','desc_3']
file_path = extract_path + '/reviews/coffee_analysis.csv'

df = load_csv(file_path)
remove_unnecessary_columns(df,columns_to_remove)
save_to_csv(df, product['data'])
