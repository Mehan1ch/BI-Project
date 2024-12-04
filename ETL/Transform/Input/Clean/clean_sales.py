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


def convert_currency_to_usd(dataframe: pandas.DataFrame, currency_column: str,
                            exchange_rate: float) -> pandas.DataFrame:
    # Change according to the exchange rate and round to 2 decimal places
    dataframe[currency_column] = dataframe[currency_column] * exchange_rate
    dataframe[currency_column] = dataframe[currency_column].round(2)
    return dataframe


def remove_milliseconds(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    dataframe['datetime'] = pandas.to_datetime(dataframe['datetime']).dt.floor('s')
    return dataframe


def add_product_category(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Add a new column to the dataframe called product_category
    # If the coffee_name is Hot Chocolate set the product_category to 'Drinking Chocolate' else Coffee
    dataframe['product_category'] = 'Coffee'
    dataframe.loc[dataframe['coffee_name'] == 'Hot Chocolate', 'product_category'] = 'Drinking Chocolate'
    return dataframe


def add_product_details(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Add a new column to the dataframe called product_detail
    # Set it to '-' everywhere
    dataframe['product_detail'] = '-'
    return dataframe


def add_quantity(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Add a new column to the dataframe called transaction_qty
    # Set it to 1 everywhere
    dataframe['transaction_qty'] = 1
    return dataframe


def rename_columns(dataframe: pandas.DataFrame, columns: {str: str}) -> pandas.DataFrame:
    dataframe.rename(columns=columns, inplace=True)
    return dataframe


def reorganize_columns(dataframe: pandas.DataFrame, columns: [str]) -> pandas.DataFrame:
    return dataframe[columns]


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


columns_to_remove = ['card', 'cash_type']
file_path = extract_path + '/sales/index.csv'
UAH_TO_USD = 0.024  # 2024 UAH to USD

df = load_csv(file_path)
df = remove_unnecessary_columns(df, columns_to_remove)
df = convert_currency_to_usd(df, 'money', UAH_TO_USD)
df = remove_milliseconds(df)
df = add_product_category(df)
df = add_product_details(df)
df = add_quantity(df)
df = rename_columns(df, {'coffee_name': 'product_type', 'money': 'unit_price', 'datetime': 'transaction_time',
                         'date': 'transaction_date'})
df = reorganize_columns(df,
                        ['transaction_date', 'transaction_time', 'transaction_qty', 'unit_price', 'product_category',
                         'product_type', 'product_detail'])
save_to_csv(df, product['data'])
