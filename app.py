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
import json
import dash_table
from flask_caching import Cache

import utils

server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = 'GNPS - Dataset Browser'

# Optionally turn on caching
if __name__ == "__main__":
    cache = Cache(app.server, config={
        'CACHE_TYPE': "null",
        #'CACHE_TYPE': 'filesystem',
        'CACHE_DIR': 'temp/flask-cache',
        'CACHE_DEFAULT_TIMEOUT': 0,
        'CACHE_THRESHOLD': 1000000
    })
else:
    WORKER_UP = True
    cache = Cache(app.server, config={
        #'CACHE_TYPE': "null",
        'CACHE_TYPE': 'filesystem',
        'CACHE_DIR': 'temp/flask-cache',
        'CACHE_DEFAULT_TIMEOUT': 0,
        'CACHE_THRESHOLD': 120
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
                dbc.NavItem(dbc.NavLink("Documentation", href="https://ccms-ucsd.github.io/GNPSDocumentation/gnpsdatasetexplorer/")),
            ],
        navbar=True)
    ],
    color="light",
    dark=False,
    sticky="top",
)

DASHBOARD = [
    dbc.CardHeader(html.H5("GNPS Dataset Dashboard - Version - 0.2")),
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
            dbc.InputGroup(
                [
                    dbc.InputGroupAddon("Dataset Password (if private MSV) - Beta Feature", addon_type="prepend"),
                    dbc.Input(id='dataset_password', placeholder="Enter Dataset Password", type="password", value=""),
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
                                    {'label': 'DEFAULT', 'value': 'DEFAULT'},
                                    {'label': 'REDU', 'value': 'REDU'},
                                    {'label': 'MASSIVE', 'value': 'MASSIVE'},
                                ],
                                searchable=False,
                                clearable=False,
                                value="DEFAULT",
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
                dbc.Col(
                    dbc.FormGroup(
                        [
                            dbc.Label("Page Size", width=4.8, style={"width":"100px"}),
                            dcc.Dropdown(
                                id='page_size',
                                options=[
                                    {'label': '10', 'value': '10'},
                                    {'label': '30', 'value': '30'},
                                    {'label': '100', 'value': '100'},
                                ],
                                searchable=False,
                                clearable=False,
                                value="10",
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
                page_size=10,
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
            html.Br(),
            html.A("GNPS Analysis Task", href="/1ad7bc366aef45ce81d2dfcca0a9a5e7")
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

def _determine_usi_list(accession, file_table_data, selected_table_data, get_all=False, private=False):
    usi_list = []

    if get_all is True:
        for i in range(len(file_table_data)):
            filename = file_table_data[i]["filename"]
            if private:
                usi = "mzspec:PRIVATE{}:{}".format(accession, filename)
            else:
                usi = "mzspec:{}:{}".format(accession, filename)

            if len(accession) == 32:
                usi = "mzspec:GNPS:TASK-{}-{}".format(accession, filename)
            
            usi_list.append(usi)
    else:
        for selected_index in selected_table_data:
            filename = file_table_data[selected_index]["filename"]
            if private:
                usi = "mzspec:PRIVATE{}:{}".format(accession, filename)
            else:
                usi = "mzspec:{}:{}".format(accession, filename)

            if len(accession) == 32:
                usi = "mzspec:GNPS:TASK-{}-{}".format(accession, filename)
            
            usi_list.append(usi)

    return usi_list

def _determine_gnps_list(accession, file_table_data, selected_table_data):
    file_list = []

    for selected_index in selected_table_data:
        filename = file_table_data[selected_index]["filename"]
        file_path = "f.{}/{}".format(accession, filename)

        if len(accession) == 32:
            file_path = "f.{}".format(filename)
        
        file_list.append(file_path)

    return file_list



@app.callback([Output('link-button', 'children')],
              [
                  Input('dataset_accession', 'value'), 
                  Input('dataset_password', 'value'), 
                  Input('file-table', 'derived_virtual_data'),
                  Input('file-table', 'derived_virtual_selected_rows'),
                  Input('file-table2', 'derived_virtual_data'),
                  Input('file-table2', 'derived_virtual_selected_rows'),
              ])
def create_link(accession, dataset_password, file_table_data, selected_table_data, file_table_data2, selected_table_data2):
    is_private = False
    if len(dataset_password) > 0:
        is_private = True

    usi_list1 = _determine_usi_list(accession, file_table_data, selected_table_data, private=is_private)
    usi_list2 = _determine_usi_list(accession, file_table_data2, selected_table_data2, private=is_private)

    usi_string1 = "\n".join(usi_list1)
    usi_string2 = "\n".join(usi_list2)

    url_params = {}
    url_params["usi"] = usi_string1
    url_params["usi2"] = usi_string2

    total_file_count = len(usi_list1) + sum(usi_list2)

    url_provenance = dbc.Button("Visualize {} Files".format(total_file_count), block=False, color="primary", className="mr-1")
    link_selected_object = dcc.Link(url_provenance, href="https://gnps-lcms.ucsd.edu/#" + urllib.parse.quote(json.dumps(url_params)) , target="_blank")

    # Selecting the max of all files
    all_usi_list = _determine_usi_list(accession, file_table_data, selected_table_data, get_all=True, private=is_private)
    all_usi_list = all_usi_list[:50] # Lets limit to 50 here
    
    url_params = {}
    url_params["usi"] = "\n".join(all_usi_list)

    link_all = dbc.Button("Visualize All {} Files".format(len(all_usi_list)), block=False, color="primary", className="mr-1")
    link_all_object = dcc.Link(link_all, href="https://gnps-lcms.ucsd.edu/#" + urllib.parse.quote(json.dumps(url_params)) , target="_blank")

    # Button for networking
    gnps_file_list = _determine_gnps_list(accession, file_table_data, selected_table_data)
    parameters = {}
    parameters["workflow"] = "METABOLOMICS-SNETS-V2"
    parameters["spec_on_server"] = ";".join(gnps_file_list)

    gnps_url = "https://gnps.ucsd.edu/ProteoSAFe/index.jsp?params="
    gnps_url = gnps_url + urllib.parse.quote(json.dumps(parameters))

    networking_button = dbc.Button("Molecular Network {} Files at GNPS".format(len(gnps_file_list)), color="primary", className="mr-1")
    networking_link = dcc.Link(networking_button, href=gnps_url, target="_blank")

    # Selection Text
    selection_text = "Selected {} Default Files and {} Comparison Files for LCMS Analysis".format(len(usi_list1), len(usi_list2))

    return [[html.Br(), html.Hr(), selection_text, html.Br(), html.Br(), link_selected_object, networking_link, link_all_object]]

@cache.memoize()
def _get_dataset_files(accession, metadata_source, dataset_password=""):
    return utils.get_dataset_files(accession, metadata_source, dataset_password=dataset_password)

@cache.memoize()
def _get_dataset_description(accession):
    return utils.get_dataset_description(accession)

# This function will rerun at any time that the selection is updated for column
@app.callback(
    [Output('file-table', 'data'), Output('file-table', 'columns'), Output('file-table2', 'data'), Output('file-table2', 'columns')],
    [Input('dataset_accession', 'value'), Input('dataset_password', 'value'), Input("metadata_source", "value")],
)
def list_files(accession, dataset_password, metadata_source):
    columns = [{"name": "filename", "id": "filename"}]
    files_df = _get_dataset_files(accession, metadata_source, dataset_password=dataset_password)

    new_columns = files_df.columns
    for column in new_columns:
        if column == "filename":
            continue
        columns.append({"name": column, "id": column})

    return [files_df.to_dict(orient="records"), columns, files_df.to_dict(orient="records"), columns]

# This function will rerun at any time that the selection is updated for column
@app.callback(
    [Output('dataset-title', 'children'), Output('dataset-details', 'children')],
    [Input('dataset_accession', 'value'), Input('dataset_password', 'value')],
)
def dataset_information(accession, dataset_password):
    try:
        dataset_title, dataset_description = _get_dataset_description(accession)
    except:
        if len(dataset_password) > 0:
            return ["Private Dataset - {}".format(accession), "Private Dataset No Description"]
        else:
            return ["Unknown Dataset - {}".format(accession), "Unknown Dataset No Description"]

    return [dataset_title, dataset_description]


@app.callback(
    [Output('file-table', 'page_size'), Output('file-table2', 'page_size')],
    [Input('page_size', 'value')],
)
def set_page_size(page_size):
    return [int(page_size), int(page_size)]



if __name__ == "__main__":
    app.run_server(debug=True, port=5000, host="0.0.0.0")
