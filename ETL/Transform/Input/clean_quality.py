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


def remove_hawaii_from_usa(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Where Country of Origin is United States (Hawaii) change to United States
    dataframe.loc[dataframe['Country of Origin'] == 'United States (Hawaii)', 'Country of Origin'] = 'United States'
    return dataframe


def use_second_year_of_harvest(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Where Harvest Year is XXXX / YYYY change to YYYY so always the second year is used
    # If it is only of form XXXX keep it as is
    dataframe['Harvest Year'] = dataframe['Harvest Year'].str.split('/').str[-1]
    return dataframe


def average_altitude(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Convert all values to string first
    dataframe['Altitude'] = dataframe['Altitude'].astype(str)

    # Replace various delimiters and remove 'm' or 'M'
    dataframe['Altitude'] = dataframe['Altitude'].str.replace(' A ', '-').str.replace(' - ', '-').str.replace('m',
                                                                                                              '').str.replace(
        'M', '').str.replace('~', '-')

    # Split the string and calculate the average if it's a range
    def calculate_average(altitude):
        parts = altitude.split('-')
        if len(parts) > 1:
            return (float(parts[0]) + float(parts[1])) / 2
        else:
            return float(parts[0])

    dataframe['Altitude'] = dataframe['Altitude'].apply(calculate_average)
    return dataframe


def shorten_tanzania(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Where Country of Origin is United Republic of Tanzania change to Tanzania
    dataframe.loc[dataframe['Country of Origin'] == 'Tanzania, United Republic Of', 'Country of Origin'] = 'Tanzania'
    return dataframe


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pd.read_csv(input_path)


columns_to_remove = ['ID', 'Lot Number', 'Mill', 'ICO Number', 'Region', 'Producer', 'Number of Bags', 'Bag Weight',
                     'In-Country Partner',
                     'Grading Date', 'Owner', 'Status', 'Defects', 'Category One Defects',
                     'Quakers', 'Color', 'Category Two Defects', 'Expiration', 'Certification Body',
                     'Certification Address', 'Certification Contact']
file_path = extract_path + '/quality/df_arabica_clean.csv'

df = load_csv(file_path)
df = remove_unnecessary_columns(df, columns_to_remove)
df = remove_hawaii_from_usa(df)
df = use_second_year_of_harvest(df)
df = average_altitude(df)
df = shorten_tanzania(df)
save_to_csv(df, product['data'])
