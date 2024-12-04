# %% tags=["parameters"]
upstream = ['clean_sales', 'clean_shop_sales']
product = None
transform_path = None

# %%
import pandas


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


def create_drinks_table(sales: pandas.DataFrame, shop_sales: pandas.DataFrame) -> pandas.DataFrame:
    # Extract relevant columns and convert to list of tuples
    sales_drinks = sales[['product_category', 'product_type', 'product_detail', 'unit_price']].dropna().apply(tuple,
                                                                                                              axis=1).tolist()
    shop_sales_drinks = shop_sales[['product_category', 'product_type', 'product_detail', 'unit_price']].dropna().apply(
        tuple, axis=1).tolist()
    drinks = sales_drinks + shop_sales_drinks
    drinks = list(set(drinks))
    drinks.sort()
    drinks = pandas.DataFrame(drinks, columns=['product_category', 'product_type', 'product_detail', 'unit_price'])
    drinks['id'] = drinks.index
    drinks.rename(columns={
        'product_category': 'category',
        'product_type': 'type',
        'product_detail': 'detail',
        'unit_price': 'price'}, inplace=True)
    drinks = drinks.reindex(columns=['id', 'category', 'type', 'detail', 'price'])
    return drinks


sales_path = transform_path + "/Clean/clean_sales.csv"
shop_sales_path = transform_path + "/Clean/clean_shop_sales.csv"

sales = load_csv(sales_path)
shop_sales = load_csv(shop_sales_path)
df = create_drinks_table(sales=sales, shop_sales=shop_sales)
save_to_csv(df, product['data'])
