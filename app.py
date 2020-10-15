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


server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])
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
    dbc.CardHeader(html.H5("GNPS Dataset Dashboard")),
    dbc.CardBody(
        [   
            dcc.Location(id='url', refresh=False),

            html.Div(id='version', children="Version - 0.1"),

            html.Br(),
            html.H3(children='GNPS/Metabolights Dataset Accession'),
            dbc.Input(className="mb-3", id='dataset_accession', placeholder="Enter Dataset ID"),
            html.Br(),
            
            html.H3(children='File List'),
            dash_table.DataTable(
                id='file-table',
                columns=[{"name": "filename", "id": "filename"}],
                data=[],
                row_selectable='multi',
                page_size= 10,
                filter_action="native",
            ),
            html.Br(),
            dcc.Loading(
                id="link-button",
                children=[html.Div([html.Div(id="loading-output-9")])],
                type="default",
            )
        ]
    )
]

BODY = dbc.Container(
    [
        dbc.Row([dbc.Col(dbc.Card(DASHBOARD)),], style={"marginTop": 30}),
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
        return "MTBLS1842"


def _get_group_usi_string(gnps_task, metadata_column, metadata_term):
    metadata_df = _get_task_metadata_df(gnps_task)
    filesummary_df = _get_task_filesummary_df(gnps_task)
    filesummary_df["filename"] = filesummary_df["full_CCMS_path"].apply(lambda x: os.path.basename(x))

    merged_df = metadata_df.merge(filesummary_df, how="left", on="filename")

    file_list = list(merged_df[merged_df[metadata_column] == metadata_term]["full_CCMS_path"])
    usi_list = ["mzspec:GNPS:TASK-{}-f.{}:scan:1".format(gnps_task, filename) for filename in file_list]
    usi_string = "\n".join(usi_list)

    return usi_string

@app.callback([Output('link-button', 'children')],
              [Input('dataset_accession', 'value'), 
              Input('file-table', 'derived_virtual_data'),
              Input('file-table', 'derived_virtual_selected_rows')])
def create_link(accession, file_table_data, selected_table_data):
    print(len(file_table_data), selected_table_data)

    usi_list = []
    for selected_index in selected_table_data:
        filename = file_table_data[selected_index]["filename"]
        usi = "mzspec:{}:{}:scan:1".format(accession, filename)
        usi_list.append(usi)
        print(file_table_data[selected_index])

    usi_string1 = "\n".join(usi_list)

    url_params = {}
    url_params["usi"] = usi_string1

    url_provenance = dbc.Button("Visualize Files", block=True, color="primary", className="mr-1")
    provenance_link_object = dcc.Link(url_provenance, href="https://gnps-lcms.ucsd.edu/?" + urllib.parse.urlencode(url_params) , target="_blank")

    # Selection Text
    selection_text = "Selected {} Files for LCMS Analysis".format(len(usi_list))

    return [[html.Hr(), selection_text, html.Br(), html.Br(), provenance_link_object]]

# This function will rerun at any time that the selection is updated for column
@app.callback(
    [Output('file-table', 'data')],
    [Input('dataset_accession', 'value')],
)
def list_files(accession):
    if "MSV" in accession:
        return [[{"filename": "X"}]]
    if "MTBLS" in accession:
        url = "https://www.ebi.ac.uk:443/metabolights/ws/studies/{}/files?include_raw_data=true".format(accession)
        r = requests.get(url)
        all_files = r.json()["study"]
        all_files = [file_obj for file_obj in all_files if file_obj["directory"] is False]
        all_files = [file_obj for file_obj in all_files if file_obj["type"] == "derived" ]

        temp_df = pd.DataFrame(all_files)
        files_df = pd.DataFrame()
        files_df["filename"] = temp_df["file"]

        return [files_df.to_dict(orient="records")]
    return [[{"filename": "X"}]]



if __name__ == "__main__":
    app.run_server(debug=True, port=5000, host="0.0.0.0")
