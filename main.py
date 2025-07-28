import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup
import json
import csv
from fake_useragent import UserAgent
from urllib.parse import quote, urlparse
import logging

# Configuration
SEARCH = 'ecosia'
PROXY = 'socks5://Y8m7m7:B4xmek@194.67.219.76:9510'
PAGES = 5
DATE = './data.csv'
RETRIES = 3  # Number of retries for failed requests

# Semaphore to limit concurrent page requests (reduced to 10 to avoid proxy overload)
sem = asyncio.Semaphore(10)
# Semaphore to limit concurrent queries to 3
query_sem = asyncio.Semaphore(3)

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class EcosiaScraper:
    """Handles scraping search results from Ecosia asynchronously with SOCKS5 proxy."""
    
    def __init__(self, proxy):
        self.proxy = proxy
        self.ua = UserAgent()

    async def fetch_page(self, url):
        """Fetches a single page with retries and a random User-Agent through the SOCKS5 proxy."""
        headers = {'User-Agent': self.ua.random}
        logging.debug(f"Requesting {url} with headers {headers}")
        for attempt in range(RETRIES):
            connector = ProxyConnector.from_url(self.proxy, rdns=True)
            try:
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with sem:
                        async with session.get(url, headers=headers) as response:
                            if response.status == 200:
                                html = await response.text()
                                logging.debug(f"Fetched {url}, HTML length: {len(html)}, First 100 chars: {html[:100]}")
                                return html
                            else:
                                logging.error(f"Failed to fetch {url}: status {response.status}")
            except Exception as e:
                logging.error(f"Attempt {attempt + 1}/{RETRIES} failed for {url}: {e}")
            finally:
                await connector.close()
            if attempt < RETRIES - 1:
                await asyncio.sleep(2)  # Increased delay to avoid proxy overload
        logging.error(f"All {RETRIES} attempts failed for {url}")
        return None

    def parse_page(self, html, keyword, page):
        """Parses HTML to extract search results, skipping sponsored blocks."""
        if html is None:
            logging.debug(f"No HTML for page {page} of keyword {keyword}")
            return {"page": page, "results": []}
        
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Debug: Log all potential result containers
        logging.debug(f"Found {len(soup.select('div'))} divs on page {page}")
        
        # Select result containers (flexible selector based on HTML analysis)
        for result in soup.select('div:not([class*="ad"]):not([class*="cookie"])'):
            # Skip sponsored results
            if result.find(text=lambda t: t and 'provided by Google' in t.lower()):
                logging.debug(f"Skipped sponsored result: {result.text[:50]}...")
                continue
                
            link_tag = result.find('a', href=True)
            if not link_tag or not link_tag['href'].startswith('http'):
                logging.debug(f"No valid link in result: {result.text[:50]}...")
                continue
            link = link_tag['href']
            title = link_tag.text.strip() if link_tag.text.strip() else "No title"

            # Description is often in a div after the link
            description_tag = result.find('div', class_=lambda x: x and ('description' in x.lower() or 'snippet' in x.lower() or 'result' in x.lower()))
            description = description_tag.text.strip() if description_tag else ""

            # Generate favicon path
            domain = urlparse(link).netloc
            favicon_path = f"https://www.google.com/s2/favicons?domain={domain}"

            results.append({
                "title": title,
                "link": link,
                "description": description,
                "favicon_path": favicon_path,
                "keyword": keyword
            })
            logging.debug(f"Parsed result: {title[:50]}... | Link: {link[:50]}...")

        logging.info(f"Extracted {len(results)} results for page {page} of keyword {keyword}")
        return {"page": page, "results": results}

    async def scrape_query(self, query):
        """Scrapes all pages for a given query and returns structured data."""
        tasks = []
        for page in range(1, PAGES + 1):
            url = (f"https://www.ecosia.org/search?q={quote(query)}"
                   if page == 1 else
                   f"https://www.ecosia.org/search?q={quote(query)}&p={page-1}")
            tasks.append(self.fetch_page(url))
        
        html_pages = await asyncio.gather(*tasks, return_exceptions=True)
        structured_data = []
        for page_num, html in enumerate(html_pages, start=1):
            if isinstance(html, Exception):
                logging.error(f"Error in page {page_num} for {query}: {html}")
                html = None
            parsed = self.parse_page(html, query, page_num)
            structured_data.append(parsed)
        
        return {query: structured_data}

class ResultSaver:
    """Saves search results to JSON files."""
    
    def save_to_json(self, data, filename):
        """Writes data to a JSON file with proper encoding."""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Saved {filename} with {sum(len(page['results']) for page in list(data.values())[0])} results")

async def main():
    """Main function to process queries and save results."""
    scraper = EcosiaScraper(PROXY)
    saver = ResultSaver()
    
    # Read queries from CSV
    with open(DATE, 'r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=';')
        queries = [row[0] for row in reader if row]  # First column only

    async def process_query(query):
        """Processes a single query with concurrency control."""
        async with query_sem:
            data = await scraper.scrape_query(query)
            # Sanitize filename
            safe_query = ''.join(c for c in query if c.isalnum() or c in (' ', '_')).replace(' ', '_')
            filename = f"{safe_query}.json"
            saver.save_to_json(data, filename)

    # Process all queries concurrently, limited by query_sem
    tasks = [process_query(query) for query in queries]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
