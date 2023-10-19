
import pandas as pd
import os

import metabolights
import zenodo
import workbench
import pxd

import requests
import requests_cache
requests_cache.install_cache('requests_cache', expire_after=86400)

def get_dataset_files(accession, metadata_source, dataset_password="", metadata_option=None):
    """This gives a pandas dataframe with files and appended metadata

    Args:
        accession ([type]): [description]
        metadata_source ([type]): [description]

    Returns:
        [type]: [description]
    """

    file_df = pd.DataFrame()

    if "MSV" in accession:
        files_df = _get_massive_files(accession, dataset_password=dataset_password)

        if metadata_source == "REDU":
            files_df = _add_redu_metadata(files_df, accession)
        elif metadata_source == "MASSIVE":
            files_df = _add_massive_metadata(files_df, accession, metadata_option=metadata_option)

    elif "MTBLS" in accession:
        all_files = metabolights._get_mtbls_files(accession)
        temp_df = pd.DataFrame(all_files)
        files_df = pd.DataFrame()
        files_df["filename"] = temp_df["file"]
        files_df = metabolights.add_mtbls_metadata(files_df, accession)

    elif "PXD" in accession:
        all_files = pxd._get_pxd_files(accession)
        files_df = pd.DataFrame(all_files)

    elif "ST" in accession:
        files_df = workbench._get_metabolomicsworkbench_files(accession)

        #if metadata_source == "REDU":
        #    files_df = _add_redu_metadata(files_df, msv_accession)
        #elif metadata_source == "MASSIVE":
        #    files_df = _add_massive_metadata(files_df, msv_accession, metadata_option=metadata_option)
    
    elif "ZENODO" in accession:
        all_files = zenodo._get_zenodo_files(accession)
        files_df = pd.DataFrame()
        files_df["filename"] = all_files

    elif len(accession) == 32:
        # We're likely looking at a uuid from GNPS, lets hit the API
        all_files = _get_gnps_task_files(accession)
        files_df = pd.DataFrame(all_files)
        files_df = _add_task_metadata(files_df, accession)

    return files_df   

def get_dataset_description(accession):
    """Getting title and description of a dataset

    Args:
        accession ([type]): [description]

    Returns:
        [type]: [description]
    """

    dataset_title = "Dataset Title - Invalid Accession"
    dataset_description = "Error Description - Invalid Accession"

    if "MSV" in accession:
        dataset_title, dataset_description = _get_massive_dataset_information(accession)

    if "MTBLS" in accession:
        dataset_title, dataset_description = metabolights._get_mtbls_dataset_information(accession)

    if "PXD" in accession:
        dataset_title, dataset_description = pxd._get_pxd_dataset_information(accession)
    
    if "ST" in accession:
        dataset_title, dataset_description = workbench._get_metabolomicsworkbench_dataset_information(accession)

    if "ZENODO" in accession:
        dataset_title, dataset_description = zenodo._get_zenodo_dataset_information(accession)

    elif len(accession) == 32:
        # We're likely looking at a uuid from GNPS, lets hit the API
        dataset_title, dataset_description = _get_gnps_task_information(accession)

    return  dataset_title, dataset_description


def _get_gnps_task_files(gnps_task):
    url = "https://gnps.ucsd.edu/ProteoSAFe/ManageParameters?task={}".format(gnps_task)
    r = requests.get(url)
    import xmltodict
    r_json = xmltodict.parse(r.text)

    all_files = []
    for parameter in r_json["parameters"]["parameter"]:
        if parameter["@name"] == "upload_file_mapping":
            filename = parameter["#text"].split("|")[1]
            all_files.append(filename)

    acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw", ".mgf"]

    all_files = [filename for filename in all_files if os.path.splitext(filename)[1].lower() in acceptable_extensions]

    output_list = []
    for filename in all_files:
        output_dict = {}
        output_dict["filename"] = filename
        output_list.append(output_dict)

    return output_list

def _get_gnps_task_information(accession):
    url = "https://gnps.ucsd.edu/ProteoSAFe/status_json.jsp?task={}".format(accession)
    r = requests.get(url)
    task_information = r.json()

    return task_information["description"], "ProteoSAFe Task {} - Workflow {} - Version {} - User {}".format(accession, task_information["workflow"], task_information["workflow_version"], task_information["user"])

def _get_massive_files(dataset_accession, dataset_password=""):
    all_files_df = pd.DataFrame()

    # Trying to use the file cache
    try:
        all_files_df = _get_massive_files_cached(dataset_accession)
    except:
        pass

    # Trying to use the HTTPS endpoint
    if len(all_files_df) == 0:
        try:
            from gnpsdata import publicdata
            all_files = publicdata.get_massive_public_dataset_filelist(dataset_accession)

            acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]

            all_files = [fileobj for fileobj in all_files if os.path.splitext(fileobj["file_descriptor"])[1].lower() in acceptable_extensions]

            all_files_df = pd.DataFrame(all_files)
            all_files_df["filepath"] = all_files_df["file_descriptor"].apply(lambda x: x.replace("f.", ""))

        except:
            pass

    if len(all_files_df) == 0:
        all_files_df = _get_massive_files_ftp(dataset_accession, dataset_password=dataset_password)

    all_files_df["filepath"] = all_files_df["filepath"].apply(lambda x: x.replace(dataset_accession + "/", "") )
    
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


def _get_massive_files_ftp(dataset_accession, dataset_password=""):
    import ftputil
    import ming_proteosafe_library

    if len(dataset_password) > 0:
        massive_host = ftputil.FTPHost("massive.ucsd.edu", dataset_accession, dataset_password)
    else:
        massive_host = ftputil.FTPHost("massive.ucsd.edu", "anonymous", "")

    all_files = ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "ccms_peak", massive_host=massive_host, dataset_password=dataset_password)
    all_files += ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "peak", massive_host=massive_host, dataset_password=dataset_password)
    all_files += ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "raw", massive_host=massive_host, dataset_password=dataset_password)

    acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]

    all_files = [filename for filename in all_files if os.path.splitext(filename)[1].lower() in acceptable_extensions]

    all_files_df = pd.DataFrame()
    all_files_df["filepath"] = all_files

    return all_files_df

def _get_massive_files_cached(dataset_accession):
    url = "https://gnps-datasetcache.ucsd.edu/datasette/database/filename.csv?_stream=on&_sort=filepath&dataset__exact={}&_size=max".format(dataset_accession)
    #url = "http://mingwangbeta.ucsd.edu:5235/datasette/database/filename.csv?_stream=on&_sort=filepath&dataset__exact={}&_size=max".format(dataset_accession)
    all_files_df = pd.read_csv(url, sep=",")

    all_files = list(all_files_df["filepath"])

    acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]
    
    all_files = [filename for filename in all_files if os.path.splitext(filename)[1].lower() in acceptable_extensions]

    all_files_df = all_files_df[all_files_df["filepath"].isin(all_files)]

    return all_files_df

def _get_massive_dataset_information(dataset_accession):
    url = "http://massive.ucsd.edu/ProteoSAFe/proxi/v0.1/datasets/{}".format(dataset_accession)
    r = requests.get(url)
    dataset_information = r.json()

    return dataset_information["title"], dataset_information["summary"]

def _accession_to_msv_accession(accession):
    msv_accession = accession

    if "ST" in accession:
        url = "https://massive.ucsd.edu/ProteoSAFe/QueryDatasets?task=N%2FA&file=&pageSize=30&offset=0&query=%257B%2522full_search_input%2522%253A%2522%2522%252C%2522table_sort_history%2522%253A%2522createdMillis_dsc%2522%252C%2522query%2522%253A%257B%257D%252C%2522title_input%2522%253A%2522{}%2522%257D&target=&_=1606254845533".format(accession)
        r = requests.get(url)
        data_json = r.json()

        msv_accession = data_json["row_data"][0]["dataset"]
    
    return msv_accession




def _add_redu_metadata(files_df, accession):
    redu_metadata = pd.read_csv("https://redu.ucsd.edu/dump", sep='\t')
    files_df["filename"] = "f." + accession + "/" + files_df["filename"]
    files_df = files_df.merge(redu_metadata, how="left", on="filename")
    files_df = files_df[["filename", "MassSpectrometer", "SampleType", "SampleTypeSub1"]]
    files_df["filename"] = files_df["filename"].apply(lambda x: x.replace("f.{}/".format(accession), ""))

    return files_df


def _get_massive_metadata_options(accession):
    dataset_information = requests.get("https://massive.ucsd.edu/ProteoSAFe/MassiveServlet?function=massiveinformation&massiveid={}&_=1601057558273".format(accession)).json()
    dataset_task = dataset_information["task"]

    url = "https://massive.ucsd.edu/ProteoSAFe/result_json.jsp?task={}&view=view_metadata_list".format(dataset_task)
    metadata_list = requests.get("https://massive.ucsd.edu/ProteoSAFe/result_json.jsp?task={}&view=view_metadata_list".format(dataset_task)).json()["blockData"]

    return metadata_list

def _add_massive_metadata(files_df, accession, metadata_option=None):
    try:
        # Getting massive task from accession
        metadata_list = _get_massive_metadata_options(accession)

        if len(metadata_list) == 0:
            return files_df
        
        if metadata_option is not None and len(metadata_option) > 0:
            metadata_filename = [metadata_file["File_descriptor"] for metadata_file in metadata_list if metadata_file["File_descriptor"] == metadata_option][0]
        else:
            metadata_filename = metadata_list[0]["File_descriptor"]

        ftp_url = "ftp://massive.ucsd.edu/{}".format(metadata_filename.replace("f.", ""))

        metadata_df = pd.read_csv(ftp_url, sep=None)
        # Clean the filename path
        metadata_df["filename"] = metadata_df["filename"].apply(lambda x: os.path.basename(x))

        files_df["fullfilename"] = files_df["filename"]
        files_df["filename"] = files_df["filename"].apply(lambda x: os.path.basename(x))
        files_df = files_df.merge(metadata_df, how="left", on="filename")

        files_df["filename"] = files_df["fullfilename"]
        files_df = files_df.drop("fullfilename", axis=1)
    except:
        pass

    return files_df

def _add_task_metadata(files_df, task):
    try:
        # Trying to get classical network metadata
        url = "https://gnps.ucsd.edu/ProteoSAFe/result_json.jsp?task={}&view=view_metadata".format(task)
        metadata_df = pd.DataFrame(requests.get(url).json()["blockData"])

        files_df["fullfilename"] = files_df["filename"]
        files_df["filename"] = files_df["fullfilename"].apply(lambda x: os.path.basename(x))
        
        metadata_df["filename"] = metadata_df["_dyn_#filename"].apply(lambda x: x.replace("_dyn_#", ""))
        files_df = files_df.merge(metadata_df, how="left", on="filename")
        files_df["filename"] = files_df["fullfilename"]

        files_df = files_df.drop("fullfilename", axis=1)
        files_df = files_df.drop("_dyn_#filename", axis=1)
    except:
        pass

    return files_df