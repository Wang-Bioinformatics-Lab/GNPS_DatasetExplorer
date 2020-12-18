import sys
import os
sys.path.insert(0, "..")
import utils

def test_msv():
    accession = "MSV000086206"
    print(utils.get_dataset_files(accession, "REDU"))

def test_mtbls():
    accession = "MTBLS1842"
    print(utils.get_dataset_files(accession, "REDU"))

def test_pride():
    accession = "PXD005011"
    print(utils.get_dataset_files(accession, "REDU"))
    print(utils.get_dataset_description(accession))