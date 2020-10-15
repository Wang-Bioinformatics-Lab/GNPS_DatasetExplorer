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
    dbc.CardHeader(html.H5("GNPS Dataset Dashboard")),
    dbc.CardBody(
        [   
            dcc.Location(id='url', refresh=False),

            html.Div(id='version', children="Version - 0.1"),

            html.Br(),
            html.H3(children='GNPS/Metabolights Dataset Accession'),
            dbc.Input(className="mb-3", id='dataset_accession', placeholder="Enter Dataset ID"),
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
                                    "width":"60%"
                                }
                            )
                        ],
                        row=True,
                        className="mb-3",
                    )),
            ]),
            
            html.H3(children='File List'),
            dash_table.DataTable(
                id='file-table',
                columns=[{"name": "filename", "id": "filename"}],
                data=[],
                row_selectable='multi',
                page_size= 20,
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
        return "MSV000086206"
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

    return [[html.Br(), html.Hr(), selection_text, html.Br(), html.Br(), provenance_link_object]]


@cache.memoize()
def _get_massive_files(dataset_accession):
    import ftputil
    import ming_proteosafe_library

    massive_host = ftputil.FTPHost("massive.ucsd.edu", "anonymous", "")

    all_files = ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "ccms_peak", massive_host=massive_host)
    all_files += ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "peak", massive_host=massive_host)

    return all_files

@cache.memoize()
def _get_mtbls_files(dataset_accession):
    url = "https://www.ebi.ac.uk:443/metabolights/ws/studies/{}/files?include_raw_data=true".format(dataset_accession)
    r = requests.get(url)
    all_files = r.json()["study"]
    all_files = [file_obj for file_obj in all_files if file_obj["directory"] is False]
    all_files = [file_obj for file_obj in all_files if file_obj["type"] == "derived" ]
    
    return all_files

def _add_redu_metadata(files_df, accession):
    redu_metadata = pd.read_csv("https://redu.ucsd.edu/dump", sep='\t')
    files_df["filename"] = "f." + accession + "/" + files_df["filename"]
    files_df = files_df.merge(redu_metadata, how="left", on="filename")
    files_df = files_df[["filename", "MassSpectrometer", "SampleType", "SampleTypeSub1"]]
    files_df["filename"] = files_df["filename"].apply(lambda x: x.replace("f.{}/".format(accession), ""))
    print(files_df.head())

    return files_df

def _add_massive_metadata(files_df, accession):
    try:
        # Getting massive task from accession
        dataset_information = requests.get("https://massive.ucsd.edu/ProteoSAFe/MassiveServlet?function=massiveinformation&massiveid={}&_=1601057558273".format(accession)).json()
        dataset_task = dataset_information["task"]

        url = "https://massive.ucsd.edu/ProteoSAFe/result_json.jsp?task={}&view=view_metadata_list".format(dataset_task)
        metadata_list = requests.get("https://massive.ucsd.edu/ProteoSAFe/result_json.jsp?task={}&view=view_metadata_list".format(dataset_task)).json()["blockData"]
        if len(metadata_list) == 0:
            return files_df
        
        metadata_filename = metadata_list[0]["File_descriptor"]
        ftp_url = "ftp://massive.ucsd.edu/{}".format(metadata_filename.replace("f.", ""))

        metadata_df = pd.read_csv(ftp_url, sep=None)

        files_df["fullfilename"] = files_df["filename"]
        files_df["filename"] = files_df["filename"].apply(lambda x: os.path.basename(x))
        files_df = files_df.merge(metadata_df, how="left", on="filename")

        files_df["filename"] = files_df["fullfilename"]
        files_df = files_df.drop("fullfilename", axis=1)

        print(metadata_df)
        print(files_df)
    except:
        raise

    return files_df

    


# This function will rerun at any time that the selection is updated for column
@app.callback(
    [Output('file-table', 'data'), Output('file-table', 'columns')],
    [Input('dataset_accession', 'value'), Input("metadata_source", "value")],
)
def list_files(accession, metadata_source):
    columns = [{"name": "filename", "id": "filename"}]
    if "MSV" in accession:
        all_files = _get_massive_files(accession)
        all_files = [filename.replace(accession + "/", "") for filename in all_files]

        files_df = pd.DataFrame()
        files_df["filename"] = all_files

        if metadata_source == "REDU":
            files_df = _add_redu_metadata(files_df, accession)
        elif metadata_source == "MASSIVE":
            files_df = _add_massive_metadata(files_df, accession)

        columns = [{"name": column, "id": column} for column in files_df.columns]

        return [files_df.to_dict(orient="records"), columns]
    if "MTBLS" in accession:
        all_files = _get_mtbls_files(accession)
        temp_df = pd.DataFrame(all_files)
        files_df = pd.DataFrame()
        files_df["filename"] = temp_df["file"]

        return [files_df.to_dict(orient="records"), columns]
    return [[{"filename": "X"}], columns]



if __name__ == "__main__":
    app.run_server(debug=True, port=5000, host="0.0.0.0")
