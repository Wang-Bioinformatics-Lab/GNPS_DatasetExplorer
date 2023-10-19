import json
import requests
from remotezip import RemoteZip


def _get_zenodo_files(dataset_accession):
    zenodo_id = dataset_accession.replace("ZENODO", "").replace("-", "")

    zenodo_url = "https://zenodo.org/api/records/{}".format(zenodo_id)
    r = requests.get(zenodo_url)

    all_filenames = []

    acceptable_extensions = (".raw", '.mzml', '.mzxml')

    all_files = r.json()['files']
    for file in all_files:
        if file['filename'].lower().endswith(acceptable_extensions):
            all_filenames.append(file['filename'])

        if file['filename'].endswith('.zip'):
            url = file['links']['download']
            zip_filename = file['filename']

            # Finding all the filenames
            with RemoteZip(url) as zip:
                for zip_info in zip.infolist():
                    actual_filename = zip_info.filename

                    if "__MACOSX" in actual_filename:
                        continue

                    if actual_filename.endswith(".raw") or \
                        actual_filename.endswith(".mzML") or \
                        actual_filename.endswith(".mzXML"):
                        
                        all_filenames.append("{}-{}".format(zip_filename, actual_filename))

    return all_filenames

def _get_zenodo_dataset_information(dataset_accession):
    zenodo_id = dataset_accession.replace("ZENODO", "").replace("-", "")

    zenodo_url = "https://zenodo.org/api/records/{}".format(zenodo_id)

    dataset_information = requests.get(zenodo_url).json()

    title = dataset_information["metadata"]["title"]
    description = dataset_information["metadata"]["description"]

    return title, description
