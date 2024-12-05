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
        engine = create_engine(db_url)
        query = """
        SELECT
            shop_sales.transaction_date,
            drinks.type AS drink_name,
            drinks.category AS drink_category,
            SUM(shop_sales.transaction_qty) AS total_qty,
            SUM(shop_sales.transaction_qty * drinks.price) AS total_profit
        FROM shop_sales
        JOIN drinks ON shop_sales.drink_id = drinks.id
        GROUP BY shop_sales.transaction_date, drinks.type, drinks.category
        ORDER BY shop_sales.transaction_date
        """
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as err:
        print(f"Error: {err}")
        return pd.DataFrame()


def render():
    df_sales = fetch_sales_data()
    df_coffee = df_sales[df_sales['drink_category'] == 'Coffee']
    drink_options = [{'label': drink, 'value': drink} for drink in df_coffee['drink_name'].unique()]

    return dmc.Container(
        [
            html.H1("Sales Data"),
            dcc.Dropdown(
                id='drink-dropdown',
                options=drink_options,
                placeholder="Select a drink",
                style={'width': '50%', 'margin': '0 auto'}
            ),
            html.Div(id='kpi-avg-qty', style={'fontSize': 24, 'margin': '10px 0'}),
            html.Div(id='kpi-avg-profit', style={'fontSize': 24, 'margin': '10px 0'}),
            dmc.Grid(
                [
                    dmc.GridCol(
                        dcc.Graph(
                            id='sales-quantity-graph',
                            figure={
                                'data': [],
                                'layout': {
                                    'title': 'Quantity Sold Over Time'
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
                                    'title': 'Profit Made Over Time'
                                }
                            }
                        ),
                        span=6
                    )
                ]
            ),
            dmc.Grid(
                [
                    dmc.GridCol(
                        dcc.Graph(
                            id='sales-category-graph',
                            figure={
                                'data': [],
                                'layout': {
                                    'title': 'Daily Sales Quantity by Category'
                                }
                            }
                        ),
                        span=12
                    )
                ]
            )
        ],
        style={'padding': '20px', 'margin': '0 auto', 'maxWidth': '90%'}
    )


@callback(
    Output('sales-quantity-graph', 'figure'),
    Output('sales-profit-graph', 'figure'),
    Output('sales-category-graph', 'figure'),
    Output('kpi-avg-qty', 'children'),
    Output('kpi-avg-profit', 'children'),
    Input('drink-dropdown', 'value')
)
def update_graph(selected_drink):
    df_sales = fetch_sales_data()

    # Filter data for Coffee category for the first two diagrams
    df_coffee = df_sales[df_sales['drink_category'] == 'Coffee']

    # Calculate KPIs
    if selected_drink:
        df_selected_drink = df_coffee[df_coffee['drink_name'] == selected_drink]
        avg_qty_per_day = df_selected_drink['total_qty'].mean()
        avg_daily_profit_selected = df_selected_drink.groupby('transaction_date')['total_profit'].sum().mean()
    else:
        avg_qty_per_day = df_coffee['total_qty'].mean()
        avg_daily_profit_selected = 0

    avg_daily_profit_total = df_coffee.groupby('transaction_date')['total_profit'].sum().mean()
    avg_profit_percentage = (avg_daily_profit_selected / avg_daily_profit_total) * 100 if avg_daily_profit_total else 0

    kpi_avg_qty = f"Average Quantity Sold per Day: {avg_qty_per_day:.2f}"
    kpi_avg_profit = f"Average % of Daily Profit: {avg_profit_percentage:.2f}%"

    quantity_figure = px.line(
        df_coffee,
        x='transaction_date',
        y='total_qty',
        color='drink_name',
        labels={'total_qty': 'Quantity', 'transaction_date': 'Date', 'drink_name': 'Drink Name'},
        title='Quantity Sold Over Time'
    )

    profit_figure = px.line(
        df_coffee,
        x='transaction_date',
        y='total_profit',
        color='drink_name',
        labels={'total_profit': 'Profit (USD)', 'transaction_date': 'Date', 'drink_name': 'Drink Name'},
        title='Profit Made Over Time'
    )

    category_figure = px.bar(
        df_sales,
        x='transaction_date',
        y='total_qty',
        color='drink_category',
        labels={'total_qty': 'Quantity', 'transaction_date': 'Date', 'drink_category': 'Drink Category'},
        title='Daily Sales Quantity by Category'
    )

    return quantity_figure, profit_figure, category_figure, kpi_avg_qty, kpi_avg_profit
