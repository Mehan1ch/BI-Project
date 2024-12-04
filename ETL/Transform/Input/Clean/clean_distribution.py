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

def remove_full_zero_rows(dataframe: pandas.DataFrame, columns: [str]) -> pandas.DataFrame:
    # Remove rows where all columns are 0
    remainder_columns = [col for col in dataframe.columns if col not in columns]
    return dataframe.loc[(dataframe[remainder_columns] != 0).any(axis=1)]



def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pd.read_csv(input_path)


columns_to_remove = ['Exports', 'Imports']
file_path = extract_path + '/distribution/psd_coffee.csv'

df = load_csv(file_path)
df = remove_unnecessary_columns(df, columns_to_remove)
df = remove_full_zero_rows(df, ['Country', 'Year'])
save_to_csv(df, product['data'])
