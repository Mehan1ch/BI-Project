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

columns_to_remove = ['ID','ID','Lot Number','Mill','ICO Number','Region','Producer',
                     'Grading Date','Owner','Status','Defects','Category One Defects',
                     'Quakers','Color','Category Two Defects','Expiration','Certification Body','Certification Address','Certification Contact']
file_path = extract_path + '/quality/df_arabica_clean.csv'

df = load_csv(file_path)
remove_unnecessary_columns(df,columns_to_remove)
save_to_csv(df, product['data'])
