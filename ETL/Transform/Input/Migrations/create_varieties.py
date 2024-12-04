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


def create_varieties_table(quality: pandas.DataFrame) -> pandas.DataFrame:
    # Extract farm names, remove duplicates, and ensure all are strings
    variety_list = quality['variety'].dropna().unique()
    variety_list = [str(variety) for variety in variety_list if isinstance(variety, str)]
    variety_list.sort()

    # Create DataFrame from list
    varieties = pandas.DataFrame(variety_list, columns=['name'])
    varieties['id'] = varieties.index
    varieties = varieties.reindex(columns=['id', 'name'])
    return varieties


quality_path = transform_path + "Clean/clean_quality.csv"

quality = load_csv(quality_path)
df = create_varieties_table(quality=quality)
save_to_csv(df, product['data'])
