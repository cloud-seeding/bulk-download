"""
135 Mbps
"""

import os
import json
import shutil
import asyncio
import aiohttp
import aiofiles

DOWNLOAD_LOC = "Z:/NARR/"
TEMP_DOWNLOAD_LOC = "./cache"
LIST_CATEGORY = "air"
MAX_CONCURRENT_DOWNLOADS = 20  # Adjust based on your SSD's capability


async def download_file(semaphore, session, url):
    filename = os.path.basename(url)
    temp_save_dir = os.path.join(TEMP_DOWNLOAD_LOC, LIST_CATEGORY)
    final_save_dir = os.path.join(DOWNLOAD_LOC, LIST_CATEGORY)
    temp_save_path = os.path.join(temp_save_dir, filename)
    final_save_path = os.path.join(final_save_dir, filename)
    os.makedirs(temp_save_dir, exist_ok=True)
    os.makedirs(final_save_dir, exist_ok=True)

    # Check if the file already exists in the final destination
    if os.path.exists(final_save_path):
        print(f"File already exists, skipping download: {filename}")
        return

    async with semaphore:
        try:
            # Download to SSD
            async with session.get(url) as resp:
                resp.raise_for_status()
                async with aiofiles.open(temp_save_path, 'wb') as f:
                    async for chunk in resp.content.iter_chunked(1024 * 64):
                        await f.write(chunk)
            print(f"Downloaded to SSD: {filename}")

            # Move to HDD
            shutil.move(temp_save_path, final_save_path)
            print(f"Moved to HDD: {filename}")
        except Exception as e:
            print(f"Failed to process {filename}: {e}")


async def main():
    with open('catalog.json', 'r') as f:
        data = json.load(f)
    urls = data[LIST_CATEGORY]

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    timeout = aiohttp.ClientTimeout(total=None)
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [download_file(semaphore, session, url) for url in urls]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
