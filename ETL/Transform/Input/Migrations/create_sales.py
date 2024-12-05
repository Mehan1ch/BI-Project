# %% tags=["parameters"]
upstream = ['create_drinks', 'clean_sales', 'clean_shop_sales']
product = None
transform_path = None

# %%
import pandas


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


def create_composite_key(df, columns):
    return df[columns].astype(str).agg('-'.join, axis=1)


def create_sales_table(sales: pandas.DataFrame, drinks: pandas.DataFrame) -> pandas.DataFrame:
    # Create composite keys
    drinks['composite_key'] = create_composite_key(drinks, ['category', 'type', 'detail', 'price'])
    sales['drinks_composite_key'] = create_composite_key(sales, ['product_category', 'product_type', 'product_detail',
                                                                 'unit_price'])

    # Create dictionary to map composite keys to IDs
    drink_dict = dict(zip(drinks['composite_key'], drinks['id']))

    # Map the composite keys in the sales DataFrame to their respective IDs
    sales['drink_id'] = sales['drinks_composite_key'].map(drink_dict)

    # Extract the required columns
    sales['id'] = sales.index
    sales_table = sales.reindex(columns=['id', 'transaction_date', 'transaction_time', 'transaction_qty', 'drink_id'])

    return sales_table


def create_shop_sales_table(shop_sales: pandas.DataFrame, drinks: pandas.DataFrame) -> pandas.DataFrame:
    # Create composite keys
    drinks['composite_key'] = create_composite_key(drinks, ['category', 'type', 'detail', 'price'])
    shop_sales['drinks_composite_key'] = create_composite_key(shop_sales,
                                                              ['product_category', 'product_type', 'product_detail',
                                                               'unit_price'])

    # Create dictionary to map composite keys to IDs
    drink_dict = dict(zip(drinks['composite_key'], drinks['id']))

    # Map the composite keys in the shop_sales DataFrame to their respective IDs
    shop_sales['drink_id'] = shop_sales['drinks_composite_key'].map(drink_dict)

    # Remove the transaction_id column
    shop_sales.drop('transaction_id', axis=1, inplace=True)

    # Extract the required columns
    shop_sales['id'] = shop_sales.index
    shop_sales_table = shop_sales.reindex(columns=['id', 'transaction_date', 'transaction_time', 'transaction_qty', 'drink_id'])

    return shop_sales_table


sales_path = transform_path + '/Clean/clean_sales.csv'
shop_sales_path = transform_path + '/Clean/clean_shop_sales.csv'
drinks_path = transform_path + '/Migrations/create_drinks.csv'

sales = load_csv(sales_path)
shop_sales = load_csv(shop_sales_path)
drinks = load_csv(drinks_path)

sales_table = create_sales_table(sales=sales, drinks=drinks)
shop_sales_table = create_shop_sales_table(shop_sales=shop_sales, drinks=drinks)

save_to_csv(sales_table, product['sales_data'])
save_to_csv(shop_sales_table, product['shop_sales_data'])
