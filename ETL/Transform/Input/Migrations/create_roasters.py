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


def create_roasters_table(reviews: pandas.DataFrame) -> pandas.DataFrame:
    # Extract all roasters into one csv containing id and name
    # Roaster on reviews
    roaster_list = reviews['roaster']
    roaster_list = list(set(roaster_list))
    roaster_list.sort()
    # Create DataFrame from list
    roasters = pandas.DataFrame(roaster_list, columns=['name'])
    roasters['id'] = roasters.index
    roasters = roasters.reindex(columns=['id', 'name'])
    return roasters


reviews_path = transform_path + "Clean/clean_reviews.csv"

reviews = load_csv(reviews_path)
df = create_roasters_table(reviews=reviews)
save_to_csv(df, product['data'])
