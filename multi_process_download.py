"""
95 Mbps
"""


import json
import multiprocessing
import os
import urllib.request

DOWNLOAD_LOC = "Z:/NARR/"
LIST_CATEGORY = "air"


def download_file(url):
    filename = os.path.basename(url)
    save_dir = os.path.join(DOWNLOAD_LOC, LIST_CATEGORY)
    save_path = os.path.join(save_dir, filename)
    os.makedirs(save_dir, exist_ok=True)
    try:
        urllib.request.urlretrieve(url, save_path)
        print(f"Downloaded: {url}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")


def main():
    with open('catalog.json', 'r') as f:
        data = json.load(f)
    urls = data[LIST_CATEGORY]
    with multiprocessing.Pool() as pool:
        pool.map(download_file, urls)


if __name__ == '__main__':
    main()
