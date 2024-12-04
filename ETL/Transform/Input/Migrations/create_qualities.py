# %% tags=["parameters"]
upstream = ['create_companies', 'create_varieties', 'create_farms', 'create_countries', 'create_processing_methods',
            'clean_quality']
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


def create_qualities_table(qualities: pandas.DataFrame, companies: pandas.DataFrame, varieties: pandas.DataFrame,
                           farms: pandas.DataFrame,
                           countries: pandas.DataFrame, processing_methods: pandas.DataFrame) -> pandas.DataFrame:
    # Create composite keys
    farms['composite_key'] = create_composite_key(farms, ['farm_name', 'altitude'])
    qualities['farms_composite_key'] = create_composite_key(qualities, ['farm_name', 'altitude'])

    # Create dictionaries to map composite keys to IDs
    farm_dict = dict(zip(farms['composite_key'], farms['id']))
    company_dict = dict(zip(companies['name'], companies['id']))
    country_dict = dict(zip(countries['country'], countries['id']))
    variety_dict = dict(zip(varieties['name'], varieties['id']))
    processing_method_dict = dict(zip(processing_methods['name'], processing_methods['id']))

    # Map the composite keys in the reviews DataFrame to their respective IDs
    qualities['farm_id'] = qualities['farms_composite_key'].map(farm_dict).fillna(0).astype(int)
    qualities['company_id'] = qualities['company'].map(company_dict).fillna(0).astype(int)
    qualities['country_id'] = qualities['country'].map(country_dict).fillna(0).astype(int)
    qualities['variety_id'] = qualities['variety'].map(variety_dict).fillna(0).astype(int)
    qualities['processing_method_id'] = qualities['processing_method'].map(processing_method_dict).fillna(0).astype(int)

    # Extract the required columns
    qualities['id'] = qualities.index
    qualities_table = qualities.reindex(columns=[
        'id', 'farm_id', 'company_id', 'country_id', 'variety_id', 'processing_method_id', 'harvest_year', 'aroma',
        'flavor', 'aftertaste', 'acidity', 'body', 'balance', 'uniformity', 'clean_cup', 'sweetness', 'overall',
        'total_cup_points', 'moisture_percentage'
    ])

    return qualities_table


qualities_path = transform_path + "Clean/clean_quality.csv"
farms_path = transform_path + "Migrations/create_farms.csv"
companies_path = transform_path + "Migrations/create_companies.csv"
varieties_path = transform_path + "Migrations/create_varieties.csv"
processing_methods_path = transform_path + "Migrations/create_processing_methods.csv"
countries_path = transform_path + "Migrations/create_countries.csv"

qualities = load_csv(qualities_path)
farms = load_csv(farms_path)
companies = load_csv(companies_path)
varieties = load_csv(varieties_path)
processing_methods = load_csv(processing_methods_path)
countries = load_csv(countries_path)
df = create_qualities_table(qualities=qualities, farms=farms, companies=companies, varieties=varieties,
                            processing_methods=processing_methods, countries=countries)
save_to_csv(df, product['data'])
