# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table
import plotly.express as px
from dash.dependencies import Input, Output
import os
from zipfile import ZipFile
import urllib.parse
from flask import Flask, send_from_directory

import pandas as pd
import requests
import uuid
import dash_table
from flask_caching import Cache

import utils

server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = 'GNPS - Dataset Browser'
cache = Cache(app.server, config={
    #'CACHE_TYPE': "null",
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'temp/flask-cache',
    'CACHE_DEFAULT_TIMEOUT': 0,
    'CACHE_THRESHOLD': 1000000
})
server = app.server



NAVBAR = dbc.Navbar(
    children=[
        dbc.NavbarBrand(
            html.Img(src="https://gnps-cytoscape.ucsd.edu/static/img/GNPS_logo.png", width="120px"),
            href="https://gnps.ucsd.edu"
        ),
        dbc.Nav(
            [
                dbc.NavItem(dbc.NavLink("GNPS Dataset Dashboard", href="#")),
            ],
        navbar=True)
    ],
    color="light",
    dark=False,
    sticky="top",
)

DASHBOARD = [
    dbc.CardHeader(html.H5("GNPS Dataset Dashboard - Version - 0.1")),
    dbc.CardBody(
        [   
            dcc.Location(id='url', refresh=False),
            html.Br(),
            dbc.InputGroup(
                [
                    dbc.InputGroupAddon("GNPS/Metabolights/PX Dataset Accession", addon_type="prepend"),
                    dbc.Input(id='dataset_accession', placeholder="Enter Dataset ID"),
                ],
                className="mb-3",
            ),
            html.Br(),
            dbc.Row([
                dbc.Col(
                    dbc.FormGroup(
                        [
                            dbc.Label("Metadata Source", width=4.8, style={"width":"150px"}),
                            dcc.Dropdown(
                                id='metadata_source',
                                options=[
                                    {'label': 'REDU', 'value': 'REDU'},
                                    {'label': 'MASSIVE', 'value': 'MASSIVE'}
                                ],
                                searchable=False,
                                clearable=False,
                                value="REDU",
                                style={
                                    "width":"60%",
                                }
                            )
                        ],
                        row=True,
                        className="mb-3",
                    ),
                    style={
                            "left":"20px",
                        }
                    ),
            ]),
            html.Hr(),

            html.H3(children="Dataset Title Placeholder", id='dataset-title'),
            html.Hr(),
            html.Div(children="Dataset details", id='dataset-details'),

            html.Hr(),
            
            html.H3(children='Default File Selection List'),
            html.Hr(),
            dash_table.DataTable(
                id='file-table',
                columns=[{"name": "filename", "id": "filename"}],
                data=[],
                row_selectable='multi',
                page_size= 10,
                filter_action="native",
                export_format="xlsx"
            ),
            html.Br(),
            html.Br(),

            html.H3(children='Comparison File Selection List (Optional)'),
            html.Hr(),
            dash_table.DataTable(
                id='file-table2',
                columns=[{"name": "filename", "id": "filename"}],
                data=[],
                row_selectable='multi',
                page_size= 10,
                filter_action="native",
                export_format="xlsx"
            ),
            html.Br(),


            dcc.Loading(
                id="link-button",
                children=[html.Div([html.Div(id="loading-output-9")])],
                type="default",
            ),

            html.Hr(),

            html.H3(children="Example Datasets"),
            html.Hr(),

            html.A("MassIVE Dataset with mzML Files", href="/MSV000086206"),
            html.Br(),
            html.A("MassIVE Dataset with CDF Files", href="/MSV000086521"),
            html.Br(),
            html.A("Metabolights Dataset", href="/MTBLS1842"),
            html.Br(),
            html.A("ProteoXchange Dataset", href="/PXD005011"),

        ]
    )
]

BODY = dbc.Container(
    [
        dbc.Row([
            dbc.Col(
                dbc.Card(DASHBOARD)
            ),
        ], style={"marginTop": 30}),
    ],
    className="mt-12",
)

app.layout = html.Div(children=[NAVBAR, BODY])



# This enables parsing the URL to shove a task into the qemistree id
@app.callback(Output('dataset_accession', 'value'),
              [Input('url', 'pathname')])
def determine_task(pathname):
    # Otherwise, lets use the url
    if pathname is not None and len(pathname) > 1:
        return pathname[1:]
    else:
        return "MSV000086206"
        return "MTBLS1842"


def _get_group_usi_string(gnps_task, metadata_column, metadata_term):
    metadata_df = _get_task_metadata_df(gnps_task)
    filesummary_df = _get_task_filesummary_df(gnps_task)
    filesummary_df["filename"] = filesummary_df["full_CCMS_path"].apply(lambda x: os.path.basename(x))

    merged_df = metadata_df.merge(filesummary_df, how="left", on="filename")

    file_list = list(merged_df[merged_df[metadata_column] == metadata_term]["full_CCMS_path"])
    usi_list = ["mzspec:GNPS:TASK-{}-f.{}".format(gnps_task, filename) for filename in file_list]
    usi_string = "\n".join(usi_list)

    return usi_string

def _determine_usi_list(accession, file_table_data, selected_table_data):
    usi_list = []
    for selected_index in selected_table_data:
        filename = file_table_data[selected_index]["filename"]
        usi = "mzspec:{}:{}".format(accession, filename)
        usi_list.append(usi)

    return usi_list

@app.callback([Output('link-button', 'children')],
              [Input('dataset_accession', 'value'), 
              Input('file-table', 'derived_virtual_data'),
              Input('file-table', 'derived_virtual_selected_rows'),
              Input('file-table2', 'derived_virtual_data'),
              Input('file-table2', 'derived_virtual_selected_rows'),
              ])
def create_link(accession, file_table_data, selected_table_data, file_table_data2, selected_table_data2):
    usi_list1 = _determine_usi_list(accession, file_table_data, selected_table_data)
    usi_list2 = _determine_usi_list(accession, file_table_data2, selected_table_data2)

    usi_string1 = "\n".join(usi_list1)
    usi_string2 = "\n".join(usi_list2)

    url_params = {}
    url_params["usi"] = usi_string1
    url_params["usi2"] = usi_string2

    url_provenance = dbc.Button("Visualize Files", block=True, color="primary", className="mr-1")
    provenance_link_object = dcc.Link(url_provenance, href="https://gnps-lcms.ucsd.edu/?" + urllib.parse.urlencode(url_params) , target="_blank")

    # Selection Text
    selection_text = "Selected {} Default Files and {} Comparison Files for LCMS Analysis".format(len(usi_list1), len(usi_list2))

    return [[html.Br(), html.Hr(), selection_text, html.Br(), html.Br(), provenance_link_object]]

@cache.memoize()
def _get_dataset_files(accession, metadata_source):
    return utils.get_dataset_files(accession, metadata_source)

@cache.memoize()
def _get_dataset_description(accession):
    return utils.get_dataset_description(accession)

# This function will rerun at any time that the selection is updated for column
@app.callback(
    [Output('file-table', 'data'), Output('file-table', 'columns'), Output('file-table2', 'data'), Output('file-table2', 'columns')],
    [Input('dataset_accession', 'value'), Input("metadata_source", "value")],
)
def list_files(accession, metadata_source):
    columns = [{"name": "filename", "id": "filename"}]
    files_df = _get_dataset_files(accession, metadata_source)

    return [files_df.to_dict(orient="records"), columns, files_df.to_dict(orient="records"), columns]

# This function will rerun at any time that the selection is updated for column
@app.callback(
    [Output('dataset-title', 'children'), Output('dataset-details', 'children')],
    [Input('dataset_accession', 'value')],
)
def dataset_information(accession):
    dataset_title, dataset_description = _get_dataset_description(accession)

    return [dataset_title, dataset_description]


if __name__ == "__main__":
    app.run_server(debug=True, port=5000, host="0.0.0.0")
