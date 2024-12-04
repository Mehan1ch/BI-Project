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


def create_processing_methods_table(quality: pandas.DataFrame) -> pandas.DataFrame:
    # Extract farm names, remove duplicates, and ensure all are strings
    processing_method_list = quality['processing_method'].dropna().unique()
    processing_method_list = [str(processing_method) for processing_method in processing_method_list if
                              isinstance(processing_method, str)]
    processing_method_list.sort()

    # Create DataFrame from list
    processing_methods = pandas.DataFrame(processing_method_list, columns=['name'])
    processing_methods['id'] = processing_methods.index
    processing_methods = processing_methods.reindex(columns=['id', 'name'])
    return processing_methods


quality_path = transform_path + "Clean/clean_quality.csv"

quality = load_csv(quality_path)
df = create_processing_methods_table(quality=quality)
save_to_csv(df, product['data'])
