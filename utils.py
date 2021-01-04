
import pandas as pd
import requests
import os


def get_dataset_files(accession, metadata_source):
    """This gives a pandas dataframe with files and appended metadata

    Args:
        accession ([type]): [description]
        metadata_source ([type]): [description]

    Returns:
        [type]: [description]
    """

    file_df = pd.DataFrame()

    if "MSV" in accession:
        all_files = _get_massive_files(accession)
        all_files = [filename.replace(accession + "/", "") for filename in all_files]

        files_df = pd.DataFrame()
        files_df["filename"] = all_files

        if metadata_source == "REDU":
            files_df = _add_redu_metadata(files_df, accession)
        elif metadata_source == "MASSIVE":
            files_df = _add_massive_metadata(files_df, accession)

    if "MTBLS" in accession:
        all_files = _get_mtbls_files(accession)
        temp_df = pd.DataFrame(all_files)
        files_df = pd.DataFrame()
        files_df["filename"] = temp_df["file"]

    if "PXD" in accession:
        all_files = _get_pxd_files(accession)
        files_df = pd.DataFrame(all_files)

    return files_df   

def get_dataset_description(accession):
    """Getting title and description of a dataset

    Args:
        accession ([type]): [description]

    Returns:
        [type]: [description]
    """

    dataset_title = "Dataset Title - Error"
    dataset_description = "Error Description"

    if "MSV" in accession:
        dataset_title, dataset_description = _get_massive_dataset_information(accession)

    if "MTBLS" in accession:
        dataset_title, dataset_description = _get_mtbls_dataset_information(accession)

    if "PXD" in accession:
        dataset_title, dataset_description = _get_pxd_dataset_information(accession)

    return  dataset_title, dataset_description


def _get_massive_files(dataset_accession):
    import ftputil
    import ming_proteosafe_library

    massive_host = ftputil.FTPHost("massive.ucsd.edu", "anonymous", "")

    all_files = ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "ccms_peak", massive_host=massive_host)
    all_files += ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "peak", massive_host=massive_host)
    all_files += ming_proteosafe_library.get_all_files_in_dataset_folder_ftp(dataset_accession, "raw", massive_host=massive_host)

    acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]

    all_files = [filename for filename in all_files if os.path.splitext(filename)[1].lower() in acceptable_extensions]

    return all_files

def _get_massive_dataset_information(dataset_accession):
    url = "http://massive.ucsd.edu/ProteoSAFe/proxi/v0.1/datasets/{}".format(dataset_accession)
    r = requests.get(url)
    dataset_information = r.json()

    return dataset_information["title"], dataset_information["summary"]

def _get_mtbls_files(dataset_accession):
    url = "https://www.ebi.ac.uk:443/metabolights/ws/studies/{}/files/tree?include_sub_dir=true".format(dataset_accession)
    r = requests.get(url)

    all_files = r.json()["study"]
    all_files = [file_obj for file_obj in all_files if file_obj["directory"] is False]
    all_files = [file_obj for file_obj in all_files if file_obj["type"] == "derived" ]

    acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]

    acceptable_files = []
    for file_object in all_files:
        try:
            if os.path.splitext(file_object["file"])[1].lower() in acceptable_extensions:
                acceptable_files.append(file_object)
        except:
            pass
    
    return acceptable_files

def _get_mtbls_dataset_information(dataset_accession):
    url = "https://www.ebi.ac.uk/metabolights/ws/studies/{}/description".format(dataset_accession)
    r = requests.get(url)
    description = r.json()["description"]

    url = "https://www.ebi.ac.uk/metabolights/ws/studies/{}/title".format(dataset_accession)
    r = requests.get(url)
    title = r.json()["title"]
     
    return title, description


def _get_pxd_files(dataset_accession):
    url = "http://proteomecentral.proteomexchange.org/cgi/GetDataset?ID={}&outputMode=json&test=no".format(dataset_accession)
    r = requests.get(url)
    
    acceptable_extensions = [".raw", ".mzML", ".mzXML", ".CDF", ".RAW", "cdf"]

    all_files = r.json()["datasetFiles"]
    output_list = []
    for file_object in all_files:
        remote_path = file_object["value"]
        filename, extension = os.path.splitext(remote_path)
        if extension in acceptable_extensions:
            output_dict = {}
            output_dict["filename"] = os.path.basename(remote_path)
            output_list.append(output_dict)
    
    return output_list

def _get_pxd_dataset_information(dataset_accession):
    url = "http://proteomecentral.proteomexchange.org/cgi/GetDataset?ID={}&outputMode=json&test=no".format(dataset_accession)
    r = requests.get(url)

    title = r.json()["title"]
    description = r.json()["description"]
     
    return title, description

def _add_redu_metadata(files_df, accession):
    redu_metadata = pd.read_csv("https://redu.ucsd.edu/dump", sep='\t')
    files_df["filename"] = "f." + accession + "/" + files_df["filename"]
    files_df = files_df.merge(redu_metadata, how="left", on="filename")
    files_df = files_df[["filename", "MassSpectrometer", "SampleType", "SampleTypeSub1"]]
    files_df["filename"] = files_df["filename"].apply(lambda x: x.replace("f.{}/".format(accession), ""))

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
    except:
        pass

    return files_df