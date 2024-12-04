# %% tags=["parameters"]
upstream = ['clean_reviews', 'clean_distribution', 'clean_quality']
product = None
transform_path = None

# %%
import pandas


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


def create_countries_table(reviews: pandas.DataFrame, distribution: pandas.DataFrame,
                           quality: pandas.DataFrame) -> pandas.DataFrame:
    # Extract all countries into one csv containing id and name
    # loc_country and origin on reviews, Country on distribution, Country of Origin on quality
    country_list = []
    country_list.extend(reviews['loc_country'])
    country_list.extend(reviews['origin'])
    country_list.extend(distribution['country'])
    country_list.extend(quality['country'])
    country_list = list(set(country_list))
    country_list.sort()
    # Create Dataframe from list
    countries = pandas.DataFrame(country_list, columns=['country'])
    countries['id'] = countries.index
    countries = countries.reindex(columns=['id', 'country'])
    return countries


reviews_path = transform_path + "Clean/clean_reviews.csv"
distribution_path = transform_path + "Clean/clean_distribution.csv"
quality_path = transform_path + "Clean/clean_quality.csv"

reviews = load_csv(reviews_path)
distribution = load_csv(distribution_path)
quality = load_csv(quality_path)
df = create_countries_table(reviews=reviews, distribution=distribution, quality=quality)
save_to_csv(df, product['data'])
