# %% tags=["parameters"]
upstream = ['extract']
product = None
extract_path = None
transform_path = None

# %%
import pandas


def remove_unnecessary_columns(dataframe: pandas.DataFrame, columns: [str]) -> pandas.DataFrame:
    dataframe.drop(columns=columns, inplace=True)
    return dataframe


def change_hawaii_to_usa(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # In all rows where loc_country is Hawai'I, change it to United States
    dataframe.loc[dataframe['loc_country'] == 'Hawai\'I', 'loc_country'] = 'United States'
    dataframe.loc[dataframe['origin'] == 'Hawai\'I', 'origin'] = 'United States'
    dataframe.loc[dataframe['loc_country'] == 'Hawai\'i', 'loc_country'] = 'United States'
    dataframe.loc[dataframe['origin'] == 'Hawai\'i', 'origin'] = 'United States'
    return dataframe

def shorten_congo(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # In all rows where loc_country is Congo (Kinshasa), change it to Congo
    dataframe.loc[dataframe['loc_country'] == 'Democratic Republic Of The Congo', 'loc_country'] = 'Congo'
    dataframe.loc[dataframe['origin'] == 'Democratic Republic Of The Congo', 'origin'] = 'Congo'
    return dataframe


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


columns_to_remove = ['review', 'review_date']
file_path = extract_path + '/reviews/simplified_coffee.csv'

df = load_csv(file_path)
df = remove_unnecessary_columns(df, columns_to_remove)
df = change_hawaii_to_usa(df)
df = shorten_congo(df)
save_to_csv(df, product['data'])
