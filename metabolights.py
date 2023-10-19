import requests
import ftplib
import sys
import os
import pandas as pd

BASE_URL = 'https://www.ebi.ac.uk/metabolights/ws/studies'
EBI_FTP_SERVER = 'ftp.ebi.ac.uk'
MTBLS_BASE_DIR = '/pub/databases/metabolights/studies/public'
SWAGGER_API = 'https://www.ebi.ac.uk:443/metabolights/ws'


def add_mtbls_metadata(files_df, accession):
    try:
        possible_assays, study_metadata_filename = get_active_assays(accession)
        study_metadata_df = get_study_level_metadata(accession, study_metadata_filename)

        files_df["Sample Name"] = files_df["filename"].apply(lambda x: os.path.splitext(x)[0])
        files_df = files_df.merge(study_metadata_df, how="left", on="Sample Name")
    except:
        pass

    return files_df



def get_active_assays(study_id):
    url = '{SWAGGER_API}/studies/{study_id}/files?include_raw_data=false'.format(SWAGGER_API=SWAGGER_API, study_id=study_id)

    session = requests.Session()
    response = session.get(url, timeout = 90) 
    
    try:
        data = response.json()
    except ValueError:
        print('URL response failed in getting active assays ', sys.exc_info()[0]) 
        return

    #in the event that data in not returned in the right format
    if not bool(data):
        print("Data not returned properly in getting active assays ", sys.exc_info()[0])
        return

    study_file_info = data['study']
    possible_assays = []
    study_metadata_filename = ''

    for file_dict in study_file_info:
        if file_dict['status' ] == 'active':
            if file_dict['type'] == 'metadata_sample':
                study_metadata_filename = file_dict['file']
            if file_dict['type'] == 'metadata_assay':
                possible_assays.append(file_dict["file"])        
        else:
            continue

    return(possible_assays, study_metadata_filename)

def get_study_level_metadata(study_id, study_metadata_filename):
    http_path = os.path.join("http://" + EBI_FTP_SERVER + MTBLS_BASE_DIR, study_id, study_metadata_filename)
    
    return pd.read_csv(http_path, sep="\t")
    
def _get_mtbls_dataset_information(dataset_accession):
    url = "https://www.ebi.ac.uk/metabolights/ws/studies/{}/description".format(dataset_accession)
    r = requests.get(url)
    description = r.json()["description"]

    url = "https://www.ebi.ac.uk/metabolights/ws/studies/{}/title".format(dataset_accession)
    r = requests.get(url)
    title = r.json()["title"]
     
    return title, description

def _get_mtbls_files(dataset_accession):
    url = "https://www.ebi.ac.uk:443/metabolights/ws/studies/{}/files/tree?include_sub_dir=true".format(dataset_accession)
    r = requests.get(url)

    acceptable_types = ['raw', 'derived']
    
    all_files = r.json()["study"]
    all_files = [file_obj for file_obj in all_files if file_obj["directory"] is False]
    all_files = [file_obj for file_obj in all_files if file_obj["type"] in acceptable_types]

    acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]

    acceptable_files = []
    for file_object in all_files:
        try:
            if os.path.splitext(file_object["file"])[1].lower() in acceptable_extensions:
                acceptable_files.append(file_object)
        except:
            pass
    
    return acceptable_files
