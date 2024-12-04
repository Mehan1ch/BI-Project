# %% tags=["parameters"]
upstream = ['clean_reviews', 'create_blends', 'create_roasters', 'create_countries']
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


def create_reviews_table(reviews: pandas.DataFrame, blends: pandas.DataFrame, roasters: pandas.DataFrame,
                         countries: pandas.DataFrame) -> pandas.DataFrame:
    # Create composite keys
    blends['composite_key'] = create_composite_key(blends, ['name', 'roast', '100g_usd'])
    reviews['blend_composite_key'] = create_composite_key(reviews, ['name', 'roast', '100g_USD'])

    # Create dictionaries to map composite keys to IDs
    blend_dict = dict(zip(blends['composite_key'], blends['id']))
    roaster_dict = dict(zip(roasters['name'], roasters['id']))
    country_dict = dict(zip(countries['country'], countries['id']))

    # Map the composite keys in the reviews DataFrame to their respective IDs
    reviews['blend_id'] = reviews['blend_composite_key'].map(blend_dict)
    reviews['roaster_id'] = reviews['roaster'].map(roaster_dict)
    reviews['loc_country_id'] = reviews['loc_country'].map(country_dict)
    reviews['origin_id'] = reviews['origin'].map(country_dict)

    # Extract the required columns
    reviews['id'] = reviews.index
    reviews_table = reviews.reindex(columns=['id', 'rating', 'blend_id', 'roaster_id', 'loc_country_id', 'origin_id'])

    return reviews_table


reviews_path = transform_path + "Clean/clean_reviews.csv"
blends_path = transform_path + "Migrations/create_blends.csv"
roasters_path = transform_path + "Migrations/create_roasters.csv"
countries_path = transform_path + "Migrations/create_countries.csv"

reviews = load_csv(reviews_path)
blends = load_csv(blends_path)
roasters = load_csv(roasters_path)
countries = load_csv(countries_path)
df = create_reviews_table(reviews=reviews, blends=blends, roasters=roasters, countries=countries)
save_to_csv(df, product['data'])
