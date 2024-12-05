from dash import html, dcc, Input, Output, callback
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import plotly.express as px

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


def render():
    options = fetch_data()
    if options.size == 0:
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
            dcc.Graph(id='quality-graph')
        ]
    )


@callback(
    Output('quality-graph', 'figure'),
    Input('origins-dropdown', 'value')
)
def update_graph(selected_country):
    if selected_country is None:
        return {}

    df = fetch_quality_data(selected_country)
    if df.empty:
        return {}

    fig = px.line(df, x='harvest_year', y=['acidity', 'sweetness', 'body', 'aroma'],
                  labels={'value': 'Quality Metrics', 'variable': 'Metrics', 'harvest_year': 'Harvest Year'},
                  title=f'Quality Metrics Over Time for {selected_country}')
    return fig
