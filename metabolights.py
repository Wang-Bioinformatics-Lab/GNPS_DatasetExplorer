import requests
import ftplib
import sys
import os
import pandas as pd
import re

BASE_URL = 'https://www.ebi.ac.uk/metabolights/ws/studies'
EBI_FTP_SERVER = 'ftp.ebi.ac.uk'
MTBLS_BASE_DIR = '/pub/databases/metabolights/studies/public'
SWAGGER_API = 'https://www.ebi.ac.uk:443/metabolights/ws'


def add_mtbls_metadata(files_df, accession):
    try:
        possible_assays, study_metadata_filename = get_active_assays(accession)
        assay_list = []
        for assay_metadata in possible_assays:
            study_metadata_df_single = get_study_level_metadata(accession, assay_metadata)
            assay_list.append(study_metadata_df_single)

        #add missing value columns
        all_columns = set().union(*(df.columns for df in assay_list))
        for df in assay_list:
            for col in all_columns:
                if col not in df.columns:
                    df[col] = pd.NA

        study_metadata_df = pd.concat(assay_list, ignore_index=True)     


        # Remove unnecissary columns and adapt column names
        def extract_bracketed_text(col_name):
            match = re.search(r'\[(.*?)\]', col_name)
            return match.group(1) if match else col_name
        
        study_metadata_df.columns = [extract_bracketed_text(col) for col in study_metadata_df.columns]
        
        
        desired_columns = ['Instrument', 'Instrument manufacturer', 'Raw Spectral Data File', 'Derived Spectral Data File', 'Scan polarity']
        study_metadata_df = study_metadata_df[[col for col in desired_columns if col in study_metadata_df.columns]]

        # Duplicate rows if we have mzml AND raw files
        df_study_raw = pd.DataFrame()
        if 'Raw Spectral Data File' in study_metadata_df.columns:
            study_metadata_df['Raw Spectral Data File'] = study_metadata_df['Raw Spectral Data File'].astype(str)
            df_study_raw = study_metadata_df[study_metadata_df['Raw Spectral Data File'].str.contains('\.', regex=True, na=False)].copy()
            df_study_raw['Sample Name'] = df_study_raw['Raw Spectral Data File']
            df_study_raw.drop(columns=['Raw Spectral Data File', 'Derived Spectral Data File'], inplace=True)

        df_study_mzml = pd.DataFrame()
        if 'Derived Spectral Data File' in study_metadata_df.columns:
            study_metadata_df['Derived Spectral Data File'] = study_metadata_df['Derived Spectral Data File'].astype(str)
            df_study_mzml = study_metadata_df[study_metadata_df['Derived Spectral Data File'].str.contains('\.', regex=True, na=False)].copy()
            df_study_mzml['Sample Name'] = df_study_mzml['Derived Spectral Data File']
            df_study_mzml.drop(columns=['Raw Spectral Data File', 'Derived Spectral Data File'], inplace=True)

        if len(df_study_raw) > 0 and len(df_study_mzml) > 0:
            study_metadata_df = pd.concat([df_study_mzml, df_study_raw], ignore_index=True)
        elif len(df_study_raw) > 0:
            study_metadata_df = df_study_raw
        elif len(df_study_mzml) > 0:
            study_metadata_df = df_study_mzml
        elif len(df_study_raw) == 0 and len(df_study_mzml) == 0:
            return pd.DataFrame()


        files_df["Sample Name"] = files_df["filename"]
        files_df = files_df.merge(study_metadata_df, how="left", on="Sample Name")


        desired_columns = ['filename', 'Instrument', 'Instrument manufacturer', 'Scan polarity']
        files_df = files_df[[col for col in desired_columns if col in files_df.columns]]

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
    
    study_url = "https://www.ebi.ac.uk:443/metabolights/ws/studies/public/study/" + dataset_accession

    response = requests.get(study_url)
    study_details = response.json()

    study_assays = study_details['content']['assays']

    df_assays = pd.DataFrame()
    ms_study_assays = []

    #get study assays if they are MS. There can be multiple assay tables in the same study
    for index, assay_json in enumerate(study_assays):
        if assay_json['technology'] == 'mass spectrometry':  

            # Extract headers in the correct order
            headers = [None] * len(assay_json['assayTable']['fields'])
            for key, value in assay_json['assayTable']['fields'].items():
                headers[value['index']] = value['header']        

            df = pd.DataFrame(assay_json['assayTable']['data'])
            df.columns = headers

            ms_study_assays.append(df)

    if len(ms_study_assays) > 0:

        all_columns = set()
        for df in ms_study_assays:
            all_columns.update(df.columns)

        aligned_dfs = []
        for df in ms_study_assays:
            # Add missing columns with NaN values
            for col in all_columns:
                if col not in df.columns:
                    df[col] = np.nan

        # Reorder columns and add to the aligned list
        aligned_dfs.append(df[list(all_columns)])

        df_assays = pd.concat(aligned_dfs, ignore_index=True)

        # Duplicate rows if we have mzml AND raw files
        extensions = [".mzml", ".mzxml", ".cdf", ".raw", ".wiff", ".d"]
        raw_files = df_assays[df_assays['Raw Spectral Data File'].str.lower().str.endswith(tuple(extensions), na=False)]['Raw Spectral Data File'].tolist() if 'Raw Spectral Data File' in df_assays.columns else []
        mzml_files = df_assays[df_assays['Derived Spectral Data File'].str.lower().str.endswith(tuple(extensions), na=False)]['Derived Spectral Data File'].tolist() if 'Derived Spectral Data File' in df_assays.columns else []

        combined_files = raw_files + mzml_files

        # formatting the output
        all_files = []
        for file in combined_files:
            file_dict = {}
            file_dict["filename"] = file
            all_files.append(file_dict)

        return all_files


# def _get_mtbls_files(dataset_accession):
#     url = "https://www.ebi.ac.uk:443/metabolights/ws/studies/{}/files/tree?include_sub_dir=true".format(dataset_accession)
#     r = requests.get(url)

#     acceptable_types = ['raw', 'derived']
    
#     all_files = r.json()["study"]
#     all_files = [file_obj for file_obj in all_files if file_obj["directory"] is False]
#     all_files = [file_obj for file_obj in all_files if file_obj["type"] in acceptable_types]

#     acceptable_extensions = [".mzml", ".mzxml", ".cdf", ".raw"]

#     acceptable_files = []
#     for file_object in all_files:
#         try:
#             if os.path.splitext(file_object["file"])[1].lower() in acceptable_extensions:
#                 acceptable_files.append(file_object)
#         except:
#             pass
    
#     return acceptable_files
