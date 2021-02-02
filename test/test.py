import sys
import os
sys.path.insert(0, "..")
import utils

def test_msv():
    accession = "MSV000086206"
    print(utils.get_dataset_files(accession, "REDU"))

def test_mtbls():
    accessions = ["MTBLS2053", "MTBLS1842"]
    for accession in accessions:
        utils.get_dataset_files(accession, "REDU")

def test_pride():
    accessions = ["PXD005011", "PXD001726"]
    for accession in accessions:
        utils.get_dataset_files(accession, "REDU")
        utils.get_dataset_description(accession)