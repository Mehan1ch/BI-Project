from dash import html, dcc, Input, Output, callback
from sqlalchemy import create_engine
import plotly.express as px
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection details from .env file
db_url = f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@db/{os.getenv('MYSQL_DATABASE')}"


def fetch_data():
    try:
        # Connect to the MySQL database using SQLAlchemy
        engine = create_engine(db_url)

        query = """
        SELECT countries.country
        FROM reviews
        JOIN distributions ON reviews.origin_id = distributions.country_id
        JOIN qualities ON reviews.origin_id = qualities.country_id
        JOIN countries ON reviews.origin_id = countries.id
        """

        df = pd.read_sql(query, engine)
        engine.dispose()

        if df.empty:
            return []
        return df['country'].unique()
    except Exception as err:
        print(f"Error: {err}")
        return []


def fetch_quality_data(country):
    try:
        # Connect to the MySQL database using SQLAlchemy
        engine = create_engine(db_url)

        query = f"""
        SELECT qualities.harvest_year, qualities.acidity, qualities.sweetness, qualities.body, qualities.aroma
        FROM qualities
        JOIN countries ON qualities.country_id = countries.id
        WHERE countries.country = '{country}'
        ORDER BY qualities.harvest_year
        """

        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as err:
        print(f"Error: {err}")
        return pd.DataFrame()


def fetch_review_data(country):
    try:
        # Connect to the MySQL database using SQLAlchemy
        engine = create_engine(db_url)

        query = f"""
        SELECT countries.country as loc_country_id, AVG(reviews.rating) as avg_rating
        FROM reviews
        JOIN countries ON reviews.loc_country_id = countries.id
        WHERE reviews.origin_id = (SELECT id FROM countries WHERE country = '{country}')
        GROUP BY countries.country
        """

        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as err:
        print(f"Error: {err}")
        return pd.DataFrame()


def render():
    options = fetch_data()
    if len(options) == 0:
        return html.Div(
            [
                html.H1("Origins Data"),
                html.P("No data available. Please load data using the refresh button.")
            ]
        )
    return html.Div(
        [
            html.H1("Origins Data"),
            dcc.Dropdown(
                id='origins-dropdown',
                options=[{'label': name, 'value': name} for name in options],
                placeholder="Select a country"
            ),
            dcc.Graph(
                id='quality-graph',
                figure={
                    'data': [],
                    'layout': {
                        'title': 'Quality Over Time'
                    }
                }
            ),
            dcc.Graph(
                id='review-map',
                figure={
                    'data': [],
                    'layout': {
                        'title': 'Average Reviews by Location'
                    }
                }
            )
        ]
    )


@callback(
    Output('quality-graph', 'figure'),
    Output('review-map', 'figure'),
    Input('origins-dropdown', 'value')
)
def update_graph(selected_country):
    if selected_country is None:
        return {
            'data': [],
            'layout': {
                'title': 'Quality Over Time'
            }
        }, {
            'data': [],
            'layout': {
                'title': 'Average Reviews by Location'
            }
        }

    df_quality = fetch_quality_data(selected_country)
    df_reviews = fetch_review_data(selected_country)

    quality_figure = {
        'data': [
            {'x': df_quality['harvest_year'], 'y': df_quality['acidity'], 'type': 'line', 'name': 'Acidity'},
            {'x': df_quality['harvest_year'], 'y': df_quality['sweetness'], 'type': 'line', 'name': 'Sweetness'},
            {'x': df_quality['harvest_year'], 'y': df_quality['body'], 'type': 'line', 'name': 'Body'},
            {'x': df_quality['harvest_year'], 'y': df_quality['aroma'], 'type': 'line', 'name': 'Aroma'}
        ],
        'layout': {
            'title': 'Quality Over Time'
        }
    }

    review_figure = px.scatter_geo(
        df_reviews,
        locations="loc_country_id",
        locationmode="country names",
        color="avg_rating",
        hover_name="loc_country_id",
        size="avg_rating",
        projection="natural earth",
        title="Average Reviews by Location"
    )

    return quality_figure, review_figure
