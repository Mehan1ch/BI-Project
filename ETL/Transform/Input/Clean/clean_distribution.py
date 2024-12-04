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


def remove_full_zero_rows(dataframe: pandas.DataFrame, columns: [str]) -> pandas.DataFrame:
    # Remove rows where all columns are 0
    remainder_columns = [col for col in dataframe.columns if col not in columns]
    return dataframe.loc[(dataframe[remainder_columns] != 0).any(axis=1)]


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def remove_sanaa(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # In all rows where loc_country is Yemen (Sanaa), change it to Yemen
    dataframe.loc[dataframe['Country'] == 'Yemen (Sanaa)', 'Country'] = 'Yemen'
    return dataframe


def fix_south_korea(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # In all rows where Country is "Korea, South", change it to South Korea
    dataframe.loc[dataframe['Country'] == 'Korea, South', 'Country'] = 'South Korea'
    return dataframe


def fix_kongo(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # In all rows where Country is contains "Congo", change it to Congo
    dataframe.loc[dataframe['Country'].str.contains('Congo'), 'Country'] = 'Congo'
    return dataframe


def rename_columns(dataframe: pandas.DataFrame, columns: {str: str}) -> pandas.DataFrame:
    return dataframe.rename(columns=columns)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


columns_to_remove = ['Exports', 'Imports']
file_path = extract_path + '/distribution/psd_coffee.csv'

df = load_csv(file_path)
df = remove_unnecessary_columns(df, columns_to_remove)
df = remove_full_zero_rows(df, ['Country', 'Year'])
df = remove_sanaa(df)
df = fix_south_korea(df)
df = fix_kongo(df)
df = rename_columns(df, {
    'Country': 'country',
    'Year': 'year',
    'Arabica Production': 'arabica_production',
    'Bean Exports': 'bean_exports',
    'Bean Imports': 'bean_imports',
    'Beginning Stocks': 'beginning_stocks',
    'Domestic Consumption': 'domestic_consumption',
    'Ending Stocks': 'ending_stocks',
    'Other Production': 'other_production',
    'Production': 'production',
    'Roast & Ground Exports': 'roast_and_ground_exports',
    'Roast & Ground Imports': 'roast_and_ground_imports',
    'Robusta Production': 'robusta_production',
    'Rst,Ground Dom. Consum': 'roast_ground_dom_cons',
    'Soluble Dom. Cons.': 'soluble_dom_cons',
    'Soluble Exports': 'soluble_exports',
    'Soluble Imports': 'soluble_imports',
    'Total Distribution': 'total_distribution',
    'Total Supply': 'total_supply'
})
save_to_csv(df, product['data'])
