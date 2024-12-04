# %% tags=["parameters"]
upstream = ['clean_quality']
product = None
transform_path = None

# %%
import pandas


def save_to_csv(dataframe: pandas.DataFrame, output_path: str) -> None:
    dataframe.to_csv(output_path, index=False)


def load_csv(input_path: str) -> pandas.DataFrame:
    return pandas.read_csv(input_path)


def create_farms_table(quality: pandas.DataFrame) -> pandas.DataFrame:
    # Drop rows with NaN values in 'farm_name' or 'altitude'
    quality_set = quality.dropna(subset=['farm_name', 'altitude'])

    # Extract farm names and altitudes, remove duplicates, and ensure all are strings
    farm_list = quality_set[['farm_name', 'altitude']].drop_duplicates().to_dict(orient='records')

    # Sort the list of dictionaries by 'farm_name'
    farm_list = sorted(farm_list, key=lambda x: str(x['farm_name']))

    # Create DataFrame from list
    farms = pandas.DataFrame(farm_list)
    farms['id'] = farms.index
    farms = farms.reindex(columns=['id', 'farm_name', 'altitude'])
    return farms


quality_path = transform_path + "Clean/clean_quality.csv"

quality = load_csv(quality_path)
df = create_farms_table(quality=quality)
save_to_csv(df, product['data'])
