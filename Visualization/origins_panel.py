from dash import html, dcc, Input, Output, callback
from sqlalchemy import create_engine
import dash_mantine_components as dmc
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
        SELECT qualities.harvest_year, 
               AVG(qualities.acidity) as acidity, 
               AVG(qualities.sweetness) as sweetness, 
               AVG(qualities.body) as body, 
               AVG(qualities.aroma) as aroma
        FROM qualities
        JOIN countries ON qualities.country_id = countries.id
        WHERE countries.country = '{country}'
        GROUP BY qualities.harvest_year
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
        SELECT countries.country as loc_country_id, 
               AVG(reviews.rating) as avg_rating,
               AVG(blends.100g_usd) as avg_100g_usd
        FROM reviews
        JOIN countries ON reviews.loc_country_id = countries.id
        JOIN blends ON reviews.blend_id = blends.id
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
        return dmc.Container(
            [
                html.H1("Origins Data"),
                html.P("No data available. Please load data using the refresh button.")
            ],
            style={'padding': '20px', 'margin': '0 auto', 'maxWidth': '90%'}
        )
    return dmc.Container(
        [
            html.H1("Origins Data"),
            dmc.Container(
                dcc.Dropdown(
                    id='origins-dropdown',
                    options=[{'label': name, 'value': name} for name in options],
                    placeholder="Select a country"
                ),
                style={'width': '50%', 'margin': '0 auto'}
            ),
            html.Div(id='kpi-avg-100g-usd', style={'fontSize': 24, 'margin': '10px 0'}),
            dmc.Grid(
                [
                    dmc.GridCol(
                        dcc.Graph(
                            id='quality-graph',
                            figure={
                                'data': [],
                                'layout': {
                                    'title': 'Quality Over Time'
                                }
                            }
                        ),
                        span=6
                    ),
                    dmc.GridCol(
                        dcc.Graph(
                            id='review-map',
                            figure={
                                'data': [],
                                'layout': {
                                    'title': 'Average Reviews by Location'
                                }
                            }
                        ),
                        span=6
                    )
                ]
            )
        ],
        style={'padding': '20px', 'margin': '0 auto', 'maxWidth': '90%'}
    )


@callback(
    Output('quality-graph', 'figure'),
    Output('review-map', 'figure'),
    Output('kpi-avg-100g-usd', 'children'),
    Input('origins-dropdown', 'value')
)
def update_graph(selected_country):
    if selected_country is None:
        return {
            'data': [],
            'layout': {
                'title': 'Quality Over Time',
                'margin': {'l': 40, 'r': 40, 't': 40, 'b': 40},
                'xaxis': {'title': 'Year'},
                'yaxis': {'title': 'Quality Score'}
            }
        }, {
            'data': [],
            'layout': {
                'title': 'Average Reviews by Location',
                'margin': {'l': 40, 'r': 40, 't': 40, 'b': 40}
            }
        }, "Average 100g USD: N/A"

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
            'title': 'Quality Over Time',
            'margin': {'l': 40, 'r': 40, 't': 40, 'b': 40},
            'xaxis': {'title': 'Year'},
            'yaxis': {'title': 'Quality Score', 'range': [5, 10]}
        }
    }

    review_figure = px.choropleth(
        df_reviews,
        locations="loc_country_id",
        locationmode="country names",
        color="avg_rating",
        hover_name="loc_country_id",
        hover_data={"avg_rating": True},
        labels={"loc_country_id": "Country ID", "avg_rating": "Average Rating"},
        projection="natural earth",
        title="Average Reviews by Location"
    )
    review_figure.update_layout(
        margin={'l': 40, 'r': 40, 't': 40, 'b': 40}
    )

    avg_100g_usd = df_reviews['avg_100g_usd'].iloc[0] if not df_reviews.empty else "N/A"
    kpi_text = f"Average 100g USD: {avg_100g_usd:.2f}" if avg_100g_usd != "N/A" else "Average 100g USD: N/A"

    return quality_figure, review_figure, kpi_text
