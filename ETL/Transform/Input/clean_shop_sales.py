# %% tags=["parameters"]
import pandas

upstream = ['extract']
product = None
extract_path = None
transform_path = None

# %%
import pandas as pd


def load_xlsx(input_path: str, sheet_name: str) -> pandas.DataFrame:
    return pd.read_excel(input_path, sheet_name=sheet_name)


def remove_unnecessary_columns(dataframe: pandas.DataFrame, columns: [str]) -> pandas.DataFrame:
    dataframe.drop(columns=columns, inplace=True)
    return dataframe


def remove_bakery(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    return dataframe[dataframe['product_category'] != 'Bakery']


def clean_coffee_products(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Where product_type is Barista Espresso, set the product_type
    # to the product_details first word and make the detail_empty
    espresso_rows = dataframe.loc[dataframe['product_type'] == 'Barista Espresso']
    espresso_rows['product_type'] = espresso_rows['product_detail'].str.split(' ').str[0]
    espresso_rows['product_detail'] = '-'
    # Where the product_type contains the word 'brewed' and the product_category is 'Coffee'
    # set the type to Brew Coffee and
    # set the product_detail to the product_details first word
    brew_rows = dataframe.loc[(dataframe['product_type'].str.contains('brewed', case=False)) & (
            dataframe['product_category'] == 'Coffee')]
    brew_rows['product_detail'] = brew_rows['product_detail'].str.split(' ').str[0]
    brew_rows['product_type'] = 'Brew Coffee'
    # Remove detail on Drip Coffee
    drip_rows = dataframe.loc[dataframe['product_type'] == 'Drip coffee']
    drip_rows['product_type'] = 'Drip Coffee'
    drip_rows['product_detail'] = '-'
    dataframe.update(espresso_rows)
    dataframe.update(brew_rows)
    dataframe.update(drip_rows)
    return dataframe


def clean_teas(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # In every teas type colum keep the second word and capitalize the first letter and
    # set the product_detail to '-'
    tea_rows = dataframe.loc[dataframe['product_category'] == 'Tea']
    tea_rows['product_type'] = tea_rows['product_type'].str.split(' ').str[1].str.capitalize()
    tea_rows['product_detail'] = '-'
    dataframe.update(tea_rows)
    return dataframe


def clean_hot_chocolate(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # For every Drinking Chocolate Category set the detail to '-' and the type to 'Hot Chocolate'
    hot_chocolate_rows = dataframe.loc[dataframe['product_category'] == 'Drinking Chocolate']
    hot_chocolate_rows['product_type'] = 'Hot Chocolate'
    hot_chocolate_rows['product_detail'] = '-'
    dataframe.update(hot_chocolate_rows)
    return dataframe


def change_time_to_datetime(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    # Get the transaction_date and transaction_time columns and merge them into
    # the time column and convert it to datetime
    # Concatenate the transaction_date and transaction_time columns row-wise
    dataframe['transaction_time'] = dataframe['transaction_date'].astype(str) + ' ' + dataframe[
        'transaction_time'].astype(str)
    dataframe['transaction_time'] = pd.to_datetime(dataframe['transaction_time'])
    return dataframe


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


columns_to_remove = ['store_id', 'store_location', 'product_id', 'Revenue', 'Month', 'Month.1', 'Weekday', 'Weekday.1',
                     'Hour']
file_path = extract_path + '/shop-sales/Coffee Shop Sales.xlsx'

df = load_xlsx(file_path, 'Transactions')
df = remove_unnecessary_columns(df, columns_to_remove)
df = remove_bakery(df)
df = clean_coffee_products(df)
df = clean_teas(df)
df = clean_hot_chocolate(df)
df = change_time_to_datetime(df)
save_to_csv(df, product['data'])
