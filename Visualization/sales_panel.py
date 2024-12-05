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


def fetch_sales_data():
    try:
        # Connect to the MySQL database using SQLAlchemy
        engine = create_engine(db_url)

        query = """
        SELECT
            shop_sales.transaction_date,
            drinks.type AS drink_name,
            SUM(shop_sales.transaction_qty) AS total_qty,
            SUM(shop_sales.transaction_qty * drinks.price) AS total_profit
        FROM shop_sales
        JOIN drinks ON shop_sales.drink_id = drinks.id
        WHERE drinks.category = 'Coffee'
        GROUP BY shop_sales.transaction_date, drinks.type
        ORDER BY shop_sales.transaction_date
        """

        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as err:
        print(f"Error: {err}")
        return pd.DataFrame()


def render():
    return dmc.Container(
        [
            html.H1("Sales Data"),
            dmc.Grid(
                [
                    dmc.GridCol(
                        dcc.Graph(
                            id='sales-quantity-graph',
                            figure={
                                'data': [],
                                'layout': {
                                    'title': 'Relative Quantity Sold Over Time'
                                }
                            }
                        ),
                        span=6
                    ),
                    dmc.GridCol(
                        dcc.Graph(
                            id='sales-profit-graph',
                            figure={
                                'data': [],
                                'layout': {
                                    'title': 'Relative Profit Made Over Time'
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
    Output('sales-quantity-graph', 'figure'),
    Output('sales-profit-graph', 'figure'),
    Input('sales-quantity-graph', 'id')  # Dummy input to trigger the callback
)
def update_graph(_):
    df_sales = fetch_sales_data()

    # Calculate daily totals
    daily_totals = df_sales.groupby('transaction_date').agg({'total_qty': 'sum', 'total_profit': 'sum'}).reset_index()
    df_sales = df_sales.merge(daily_totals, on='transaction_date', suffixes=('', '_daily'))

    # Calculate relative values
    df_sales['relative_qty'] = df_sales['total_qty'] / df_sales['total_qty_daily']
    df_sales['relative_profit'] = df_sales['total_profit'] / df_sales['total_profit_daily']

    quantity_figure = px.line(
        df_sales,
        x='transaction_date',
        y='relative_qty',
        color='drink_name',
        labels={'relative_qty': 'Relative Quantity', 'transaction_date': 'Date'},
        title='Relative Quantity Sold Over Time'
    )

    profit_figure = px.line(
        df_sales,
        x='transaction_date',
        y='relative_profit',
        color='drink_name',
        labels={'relative_profit': 'Relative Profit', 'transaction_date': 'Date'},
        title='Relative Profit Made Over Time'
    )

    return quantity_figure, profit_figure
