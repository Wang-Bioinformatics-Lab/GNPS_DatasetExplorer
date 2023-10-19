import requests

def _get_pxd_dataset_information(dataset_accession):
    url = "http://proteomecentral.proteomexchange.org/cgi/GetDataset?ID={}&outputMode=json&test=no".format(dataset_accession)
    r = requests.get(url)

    title = r.json()["title"]
    description = r.json()["description"]
     
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


