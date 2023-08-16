import requests

SERVER_URL = 'https://explorer.gnps2.org'

def test_dataset():
    url = SERVER_URL + '/api/datasets/MSV000086873/files'
    r = requests.get(url)

    print(r.json())

    r.raise_for_status()

def test_interface():
    url = SERVER_URL
    r = requests.get(url)
    r.raise_for_status()

def main():
    test_dataset()

if __name__ == "__main__":
    main()