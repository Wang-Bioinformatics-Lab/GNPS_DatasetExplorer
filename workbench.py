import os
import requests
import pandas as pd

def _get_metabolomicsworkbench_dataset_information(dataset_accession):
    metabolomics_workbench_data = requests.get("https://www.metabolomicsworkbench.org/rest/study/study_id/{}/summary".format(dataset_accession)).json()

    return metabolomics_workbench_data["study_title"], metabolomics_workbench_data["study_summary"]

def _get_metabolomicsworkbench_files(dataset_accession):
    try:
        dataset_list_url = "https://www.metabolomicsworkbench.org/data/show_archive_contents_json.php?STUDY_ID={}".format(dataset_accession)
        mw_file_list = requests.get(dataset_list_url).json()

        acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]

        mw_file_list = [file_obj for file_obj in mw_file_list if os.path.splitext(file_obj["FILENAME"])[1].lower() in acceptable_extensions]
        workbench_df = pd.DataFrame(mw_file_list)
        workbench_df["filename"] = workbench_df["FILENAME"]
        workbench_df["size_mb"] = workbench_df["FILESIZE"].astype(int) / 1024 / 1024
        workbench_df["size_mb"] = workbench_df["size_mb"].astype(int)
        workbench_df = workbench_df[["filename", "size_mb"]]
    except:
        workbench_df = pd.DataFrame()
    
    return workbench_df