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


def create_sales_table(sales: pandas.DataFrame, shop_sales: pandas.DataFrame,
                       drinks: pandas.DataFrame) -> pandas.DataFrame:
    # Create composite keys
    drinks['composite_key'] = create_composite_key(drinks, ['category', 'type', 'detail', 'price'])
    sales['drinks_composite_key'] = create_composite_key(sales, ['product_category', 'product_type', 'product_detail',
                                                                 'unit_price'])
    shop_sales['drinks_composite_key'] = create_composite_key(shop_sales,
                                                              ['product_category', 'product_type', 'product_detail',
                                                               'unit_price'])

    # Create dictionaries to map composite keys to IDs
    drink_dict = dict(zip(drinks['composite_key'], drinks['id']))

    # Map the composite keys in the reviews DataFrame to their respective IDs
    sales['drink_id'] = sales['drinks_composite_key'].map(drink_dict)
    shop_sales['drink_id'] = shop_sales['drinks_composite_key'].map(drink_dict)

    # Append the shop_sales to sales
    sales = pandas.concat([sales, shop_sales])

    # Remove the transaction_id column
    sales.drop('transaction_id', axis=1, inplace=True)

    # Extract the required columns
    sales['id'] = sales.index
    sales_table = sales.reindex(columns=['id', 'transaction_date', 'transaction_time', 'transaction_qty', 'drink_id'])

    return sales_table


sales_path = transform_path + "/Clean/clean_sales.csv"
shop_sales_path = transform_path + "/Clean/clean_shop_sales.csv"
drinks_path = transform_path + "/Migrations/create_drinks.csv"

sales = load_csv(sales_path)
shop_sales = load_csv(shop_sales_path)
drinks = load_csv(drinks_path)
df = create_sales_table(sales=sales, shop_sales=shop_sales, drinks=drinks)
save_to_csv(df, product['data'])
