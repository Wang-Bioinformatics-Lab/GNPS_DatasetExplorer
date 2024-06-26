import os
import requests
import pandas as pd

def _get_metabolomicsworkbench_dataset_information(dataset_accession):
    metabolomics_workbench_data = requests.get("https://www.metabolomicsworkbench.org/rest/study/study_id/{}/summary".format(dataset_accession)).json()

    return metabolomics_workbench_data["study_title"], metabolomics_workbench_data["study_summary"]

def _get_metabolomicsworkbench_files_cached(dataset_accession):
    url = "https://datasetcache.gnps2.org/datasette/database/filename.csv?_stream=on&_sort=filepath&dataset__exact={}&_size=max".format(dataset_accession)
    all_files_df = pd.read_csv(url, sep=",")

    all_files = list(all_files_df["filepath"])

    acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw", ".d"]
    
    all_files = [filename for filename in all_files if os.path.splitext(filename)[1].lower() in acceptable_extensions]

    all_files_df = all_files_df[all_files_df["filepath"].isin(all_files)]

    return all_files_df

def _get_metabolomicsworkbench_files(dataset_accession):
    # Trying the cache to be a lot faster
    try:
        all_files_df = _get_metabolomicsworkbench_files_cached(dataset_accession)

        if len(all_files_df) > 0:
            files_df = pd.DataFrame()
            files_df["filename"] = all_files_df["filepath"]

            # Adding more information if possible
            if "collection" in all_files_df:
                files_df["collection"] = all_files_df["collection"]
                
            if "update_name" in all_files_df:
                files_df["update_name"] = all_files_df["update_name"]

            if "size_mb" in all_files_df:
                files_df["size_mb"] = all_files_df["size_mb"]
                files_df["ms2"] = all_files_df["spectra_ms2"]
                files_df["Vendor"] = all_files_df["instrument_vendor"]
                files_df["Model"] = all_files_df["instrument_model"]

            return files_df
    except:
        pass

    try:
        dataset_list_url = "https://www.metabolomicsworkbench.org/data/show_archive_contents_json.php?STUDY_ID={}".format(dataset_accession)
        mw_file_list = requests.get(dataset_list_url).json()

        acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw", ".d"]

        mw_file_list = [file_obj for file_obj in mw_file_list if os.path.splitext(file_obj["FILENAME"])[1].lower() in acceptable_extensions]
        workbench_df = pd.DataFrame(mw_file_list)
        workbench_df["filename"] = workbench_df["FILENAME"]
        workbench_df["size_mb"] = workbench_df["FILESIZE"].astype(int) / 1024 / 1024
        workbench_df["size_mb"] = workbench_df["size_mb"].astype(int)
        workbench_df = workbench_df[["filename", "size_mb"]]
    except:
        workbench_df = pd.DataFrame()
    
    return workbench_df