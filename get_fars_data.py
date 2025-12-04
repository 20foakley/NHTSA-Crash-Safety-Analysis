import asyncio
import aiohttp
import aiofiles
import os
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

base_url = os.getenv("BASE_URL")
out_dir = os.getenv("OUT_DIR")

START_YEAR = 1995
END_YEAR = 2023


async def download_file(session, url, out_path, sem, pbar):
    async with sem:
        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as response:
                    if response.status != 200:
                        raise Exception(f"HTTP status {response.status}")

                    async with aiofiles.open(out_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(4096):
                            await f.write(chunk)

                pbar.update(1)
                return

            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                else:
                    pbar.update(1)
                    print(f" Failed: {url} \n  with error: {e}")


async def main():
    os.makedirs(out_dir, exist_ok=True)

    years = range(START_YEAR, END_YEAR + 1)
    sem = asyncio.Semaphore(3)  

    async with aiohttp.ClientSession() as session:
        with tqdm(total=len(years), desc="downloading files") as pbar:
            tasks = []
            for year in years:
                url = base_url.format(year=year)
                out_path = os.path.join(out_dir, f"FARS{year}NationalCSV.zip")
                tasks.append(download_file(session, url, out_path, sem, pbar))

            await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
