# %% tags=["parameters"]
upstream = ['clean_distribution', 'create_countries']
product = None
transform_path = None

# %%
import pandas


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


def create_reviews_table(distributions: pandas.DataFrame, countries: pandas.DataFrame) -> pandas.DataFrame:
    # Create dictionaries to map  keys to IDs
    country_dict = dict(zip(countries['country'], countries['id']))

    # Map the  keys in the reviews DataFrame to their respective IDs
    distributions['country_id'] = distributions['country'].map(country_dict)

    # Extract the required columns
    distributions['id'] = distributions.index
    distributions_table = distributions.reindex(columns=
                                                ['id', 'country_id', 'year', 'arabica_production', 'bean_exports',
                                                 'bean_imports', 'beginning_stocks',
                                                 'domestic_consumption',
                                                 'ending_stocks', 'other_production', 'production',
                                                 'roast_and_ground_exports', 'roast_and_ground_imports',
                                                 'robusta_production', 'roast_ground_dom_cons', 'soluble_dom_cons',
                                                 'soluble_exports', 'soluble_imports',
                                                 'total_distribution',
                                                 'total_supply'
                                                 ])

    return distributions_table


distributions_path = transform_path + "Clean/clean_distribution.csv"
countries_path = transform_path + "Migrations/create_countries.csv"

distributions = load_csv(distributions_path)
countries = load_csv(countries_path)
df = create_reviews_table(distributions=distributions, countries=countries)
save_to_csv(df, product['data'])
