import sys
import os
sys.path.insert(0, "..")
import utils
import metabolights
import zenodo

def test_msv():
    #accession = "MSV000086206"
    #dataset_files_df = utils.get_dataset_files(accession, "REDU")
    
    accession = "MSV000086873"
    dataset_files_df = utils.get_dataset_files(accession, "REDU")

def test_msv_massive_metadata():
    accession = "MSV000089459"
    metadata_option = "f.MSV000089459/metadata/Metadata_Amulya_QE.tsv"
    dataset_files_df = utils.get_dataset_files(accession, "MASSIVE", metadata_option=metadata_option)
    print(dataset_files_df)

def test_mtbls():
    accessions = ["MTBLS2053", "MTBLS1842"]
    for accession in accessions:
        #utils.get_dataset_files(accession, "REDU")
        possible_assays, study_metadata_filename = metabolights.get_active_assays(accession)
        metadata_df = metabolights.get_study_level_metadata(accession, study_metadata_filename)

        print(metadata_df)

def test_pride():
    accessions = ["PXD005011", "PXD001726"]
    for accession in accessions:
        utils.get_dataset_files(accession, "REDU")
        utils.get_dataset_description(accession)

def test_workbench():
    accessions = ["ST001709"]

    for accession in accessions:
        all_files = utils.get_dataset_files(accession, "")
        print(all_files)

def test_zenodo():
    accessions = ["ZENODO-4989929", "ZENODO-8338511"]
    
    for accession in accessions:
        all_files = utils.get_dataset_files(accession, "")
        print(all_files)

def test_gnps_fbmn():
    task = "9c8d2902db494db39a292c13cf442dac"
    files_list = utils._get_gnps_task_files(task)

    print(files_list)

def main():
    #test_msv_massive_metadata()
    #test_zenodo()
    test_workbench()

if __name__ == "__main__":
    main()
