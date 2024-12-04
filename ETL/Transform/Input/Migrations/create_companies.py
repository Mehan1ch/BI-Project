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


def create_companies_table(quality: pandas.DataFrame) -> pandas.DataFrame:
    # Extract farm names, remove duplicates, and ensure all are strings
    company_list = quality['company']
    company_list = list(set(company_list))
    company_list.sort()

    # Create DataFrame from list
    companies = pandas.DataFrame(company_list, columns=['name'])
    companies['id'] = companies.index
    companies = companies.reindex(columns=['id', 'name'])
    return companies


quality_path = transform_path + "Clean/clean_quality.csv"

quality = load_csv(quality_path)
df = create_companies_table(quality=quality)
save_to_csv(df, product['data'])
