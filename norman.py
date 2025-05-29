import requests
from io import StringIO
import pandas as pd
import re
from urllib.parse import unquote


def _extract_file_name(url):
    # Step 1: Remove everything before and including the first occurrence of "sample/{some_number}/"
    url_after_sample = re.sub(r'.*sample/\d+/','', url)
    
    # Step 2: Remove everything from "?VersionId" onwards if it exists
    url_after_version = re.sub(r'\?VersionId.*', '', url_after_sample)
    
    return unquote(url_after_version)

def _extract_file_path(url):
    # Step 1: Remove everything before and including the first occurrence of "sample/{some_number}/"
    file_path = re.sub(r'https://files.dsfp.norman-data.eu/','', url)
        
    return unquote(file_path)

def _create_usi(row):
    return f"mzspec:NORMAN-{row['uuid']}:{row['file_paths']}"


def process_dataset_files(
    df_files,
    internal_id=None,
    uuid=None,
    filter_extensions=True,
):
    file_cols = ["data_independent", "data_dependent", "data_fullscan"]
    missing_cols = [c for c in file_cols if c not in df_files.columns]
    if missing_cols:
        print(f"Dataset {internal_id or '(unknown)'} missing columns: {missing_cols}. Skipping.")
        return pd.DataFrame()

    # reshape → one file per row
    df = (
        df_files[["sample_id"] + file_cols]
        .melt(id_vars=["sample_id"], value_name="file_urls")
        .dropna(subset=["file_urls"])
    )
    df = df[df["file_urls"].astype(str).str.strip() != ""]

    # filenames, paths
    df["file_names"] = df["file_urls"].apply(_extract_file_name)
    df["file_paths"] = df["file_urls"].apply(_extract_file_path)

    # optional metadata columns
    if internal_id is not None:
        df["internal_id"] = internal_id
    if uuid is not None:
        df["uuid"] = uuid

    # extension filter
    if filter_extensions:
        exts = (".mzml", ".mzxml", ".cdf", ".raw", ".wiff", ".d")
        df = df[df["file_names"].str.lower().str.endswith(exts)]

    # USI
    df["usi"] = df.apply(_create_usi, axis=1)

    # formatting
    df["ms_type"] = df["variable"]

    # updating column names
    df["filename"] = df["file_names"]

    # keeping only certain columns
    columns_to_keep = [
        "filename",
        "usi",
        "ms_type"
    ]

    df = df[columns_to_keep]

    return df


def _get_norman_files(dataset_accession, filter_extensions=True):
    norman_id = dataset_accession.replace("NORMAN-", "")

    #TODO:  first we go to the dataset cache

    # Here we will go directly to Norman API
    datasets_url = "https://dsfp.norman-data.eu/api/1/metastore/schemas/dataset/all"

    response = requests.get(datasets_url)
    response.raise_for_status()  # Raise an error for bad status codes
    datasets = response.json()

    # lets find the dataset with the norman_id
    datasets = [ds for ds in datasets if ds['uuid'] == norman_id]

    if len(datasets) == 0:
        raise ValueError("Dataset with accession {} not found in NORMAN".format(dataset_accession))
    
    if len(datasets) >= 1:
        internal_id = datasets[0]['internal_id']

        file_url = f"https://dsfp.norman-data.eu/data/{internal_id}/files.csv"

        response = requests.get(file_url)
        file_response = requests.get(file_url)
        file_response.raise_for_status()

        
        # Read CSV data into DataFrame
        csv_data = StringIO(file_response.text)
        df_files = pd.read_csv(csv_data)
        print(f"Fetched file data for dataset {internal_id}")

        df_files = process_dataset_files(
            df_files,
            internal_id=internal_id,
            uuid=norman_id,
            filter_extensions=filter_extensions
        )

        return df_files

        




def _get_norman_dataset_information(dataset_accession):
    norman_id = dataset_accession.replace("NORMAN-", "")

    norman_url = "https://dsfp.norman-data.eu/api/1/metastore/schemas/dataset/items/{}".format(norman_id)

    dataset_information = requests.get(norman_url).json()

    title = dataset_information["title"]
    description = dataset_information["description"]

    return title, description




