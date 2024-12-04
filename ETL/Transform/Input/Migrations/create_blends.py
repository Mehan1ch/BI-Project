# %% tags=["parameters"]
upstream = ['clean_reviews']
product = None
transform_path = None

# %%
import pandas


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


def create_blends_table(reviews: pandas.DataFrame) -> pandas.DataFrame:
    # Extract all blends into one csv containing id, name, roast, and 100g_usd
    # Blend on reviews
    blend_list = reviews[['name', 'roast', '100g_USD']].to_dict(orient='records')
    blend_list = [tuple(blend.values()) for blend in blend_list]
    blend_list = list(set(blend_list))
    blend_list.sort()
    # Create DataFrame from list
    blends = pandas.DataFrame(blend_list, columns=['name', 'roast', '100g_usd'])
    blends['id'] = blends.index
    blends = blends.reindex(columns=['id', 'name', 'roast', '100g_usd'])
    return blends


reviews_path = transform_path + "/Clean/clean_reviews.csv"

reviews = load_csv(reviews_path)
df = create_blends_table(reviews=reviews)
save_to_csv(df, product['data'])
