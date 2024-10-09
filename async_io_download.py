import os
import json
import asyncio
import aiohttp
import aiofiles

DOWNLOAD_LOC = "Z:/NARR/"
LIST_CATEGORY = "air"
MAX_CONCURRENT_DOWNLOADS = 20  # Adjust this number based on your network capability


async def download_file(session, url):
    filename = os.path.basename(url)
    save_dir = os.path.join(DOWNLOAD_LOC, LIST_CATEGORY)
    save_path = os.path.join(save_dir, filename)
    os.makedirs(save_dir, exist_ok=True)

    try:
        async with session.get(url) as resp:
            resp.raise_for_status()
            async with aiofiles.open(save_path, 'wb') as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    await f.write(chunk)
        print(f"Downloaded: {url}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")


async def main():
    with open('catalog.json', 'r') as f:
        data = json.load(f)
    urls = data[LIST_CATEGORY]

    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_DOWNLOADS)
    # Remove timeout limit if needed
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [download_file(session, url) for url in urls]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
