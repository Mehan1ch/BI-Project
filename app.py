from time import sleep

import dash_mantine_components as dmc
from dash_iconify import DashIconify
from dash import _dash_renderer
from dash import Dash, html, Input, Output, callback, clientside_callback
from ploomber.spec import DAGSpec
from Visualization.origins_panel import render as render_origins
from Visualization.sales_panel import render as render_sales

_dash_renderer._set_react_version("18.2.0")
app = Dash(external_stylesheets=dmc.styles.ALL, suppress_callback_exceptions=True)

app.layout = dmc.MantineProvider(
    html.Div(
        [
            html.H1("Coffee Dashboard", style={"textAlign": "center", "marginBottom": "20px"}),
            dmc.Affix(
                dmc.Button(
                    "Refresh Database",
                    id="loading-button",
                    leftSection=DashIconify(icon="fluent:database-plug-connected-20-filled"),
                ),
                position={"top": 20, "right": 20}
            ),
            dmc.Tabs(
                [
                    dmc.TabsList(
                        [
                            dmc.TabsTab(
                                "Origins",
                                leftSection=DashIconify(icon="mdi:location-radius"),
                                value="origins",
                            ),
                            dmc.TabsTab(
                                "Sales",
                                leftSection=DashIconify(icon="mdi:coffee"),
                                value="sales",
                            ),
                        ],
                    ),
                    html.Div(id="tabs-content", style={"paddingTop": 5}),
                ],
                value="origins",
                placement="left",
                id="tabs-component"
            )
        ]
    )
)

clientside_callback(
    """
       function updateLoadingState(n_clicks) {
           return true
       }
       """,
    Output("loading-button", "loading", allow_duplicate=True),
    Input("loading-button", "n_clicks"),
    prevent_initial_call=True,
)


@callback(
    Output("loading-button", "loading"),
    Input("loading-button", "n_clicks"),
    prevent_initial_call=True,
)
def refresh_data(n_clicks):
    # Simulate data fetching
    spec = DAGSpec('./pipeline.yaml')
    dag = spec.to_dag()
    dag.build(force=True, show_progress=True)

    # Set loading state to False after data fetching
    return False


@callback(
    Output("tabs-content", "children"),
    Input("tabs-component", "value"),
    Input("loading-button", "loading")
)
def render_content(value, loading):
    if value == "origins":
        if loading:
            return dmc.Loader(color="red", size="md", variant="oval")
        else:
            return render_origins()
    elif value == "sales":
        if loading:
            return dmc.Loader(color="blue", size="md", variant="oval")
        else:
            return render_sales()


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8050, debug=True)
