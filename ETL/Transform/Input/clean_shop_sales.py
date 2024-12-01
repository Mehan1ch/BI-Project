# %% tags=["parameters"]
import pandas

upstream = ['extract']
product = None
extract_path = None
transform_path = None

# %%
import pandas as pd

def load_xlsx(input_path: str,sheet_name:str) -> pandas.DataFrame:
    return pd.read_excel(input_path,sheet_name=sheet_name)

def remove_unnecessary_columns(dataframe: pandas.DataFrame, columns: [str]) -> pandas.DataFrame:
    dataframe.drop(columns=columns, inplace=True)
    return dataframe

def save_to_csv(df: pandas.DataFrame, output_path: str) -> None:
    df.to_csv(output_path, index=False)

columns_to_remove = ['store_id','store_location','product_id']
file_path = extract_path + '/shop-sales/Coffee Shop Sales.xlsx'

df = load_xlsx(file_path,'Transactions')
remove_unnecessary_columns(df,columns_to_remove)
save_to_csv(df, product['data'])
