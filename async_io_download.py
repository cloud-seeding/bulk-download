"""
500 Mbps
"""

import os
import json
import shutil
import asyncio
import aiohttp
import aiofiles
import re
import logging

DOWNLOAD_LOC = "Z:/NARR/"
TEMP_DOWNLOAD_LOC = "E:/cache/"
LIST_CATEGORY = "ALL"
MAX_CONCURRENT_DOWNLOADS = 20  # Adjust based on your SSD's capability
START_YEAR = 2000  # Only download files from this year onwards

# Configure logging
logging.basicConfig(filename='log.txt', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')


def extract_year_from_filename(filename):
    """
    Extracts the year from the filename using regular expressions.
    Assumes the filename contains a date in the format YYYY or YYYYMMDD.
    """
    match = re.search(r'(\d{4})', filename)
    if match:
        return int(match.group(1))
    else:
        return None


async def download_file(semaphore, session, url, category, max_retries=3):
    filename = os.path.basename(url)
    temp_save_dir = os.path.join(TEMP_DOWNLOAD_LOC, category)
    final_save_dir = os.path.join(DOWNLOAD_LOC, category)
    temp_save_path = os.path.join(temp_save_dir, filename)
    final_save_path = os.path.join(final_save_dir, filename)
    os.makedirs(temp_save_dir, exist_ok=True)
    os.makedirs(final_save_dir, exist_ok=True)

    # Check if the file already exists in the final destination
    if os.path.exists(final_save_path):
        print(f"File already exists, skipping download: {filename}")
        logging.info(f"File already exists, skipping download: {filename}")
        return

    # Initialize retry count
    retry_count = 0

    while retry_count <= max_retries:
        try:
            # Before starting download, if temp file exists, delete it
            if os.path.exists(temp_save_path):
                os.remove(temp_save_path)
                logging.info(f"Deleted partial file: {temp_save_path}")

            # Acquire semaphore before starting download
            async with semaphore:
                # Download to SSD
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(temp_save_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(1024 * 64):
                            await f.write(chunk)
            print(f"Downloaded to SSD: {filename}")
            logging.info(f"Downloaded to SSD: {filename}")

            # Move to HDD
            shutil.move(temp_save_path, final_save_path)
            print(f"Moved to HDD: {filename}")
            logging.info(f"Moved to HDD: {filename}")

            # Successful download, break out of retry loop
            break

        except Exception as e:
            retry_count += 1
            print(f"Failed to process {filename}, attempt {retry_count}: {e}")
            logging.error(f"Failed to process {
                          filename}, attempt {retry_count}: {e}")

            # Delete any partially downloaded file
            if os.path.exists(temp_save_path):
                os.remove(temp_save_path)
                logging.info(f"Deleted partial file after failure: {
                             temp_save_path}")

            if retry_count > max_retries:
                print(f"Max retries reached for {filename}, moving on.")
                logging.error(f"Max retries reached for {
                              filename}, moving on.")
                break
            else:
                # Wait some time before retrying
                await asyncio.sleep(5)  # Wait 5 seconds before retrying


async def main():
    with open('catalog.json', 'r') as f:
        data = json.load(f)

    if LIST_CATEGORY == "ALL":
        urls_with_category = []
        for category in data:
            for url in data[category]:
                urls_with_category.append((url, category))
    else:
        urls_with_category = [(url, LIST_CATEGORY)
                              for url in data[LIST_CATEGORY]]

    # Filter URLs based on START_YEAR
    filtered_urls_with_category = []
    for url, category in urls_with_category:
        filename = os.path.basename(url)
        year = extract_year_from_filename(filename)
        if year and year >= START_YEAR:
            filtered_urls_with_category.append((url, category))
        else:
            print(f"Skipping file from year {year}: {filename}")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    timeout = aiohttp.ClientTimeout(total=None)
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            download_file(semaphore, session, url, category)
            for url, category in filtered_urls_with_category
        ]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
