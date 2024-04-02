from setting import *
import requests
from requests.exceptions import RequestException
import pandas as pd
from data import DBStorage
from datetime import datetime
from urllib.parse import quote_plus
import csv
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from filter import Filter

def search_api(query, pages=int(RESULT_COUNT/10)):
    results = []
    for i in range(0, pages):
        start = i*10+1
        url = SEARCH_URL.format(
            key=SEARCH_KEY,
            cx=SEARCH_ID,
            q=quote_plus(query),
            start=start
        )
        response = requests.get(url)
        data = response.json()
        results += data['items']
    result_df = pd.DataFrame.from_dict(results)
    result_df["rank"] = list(range(1, result_df.shape[0] + 1))
    result_df = result_df[["link", "rank", "snippet", "title"]]
    return result_df

def scrape_page(links):
    texts = []
    for link in links:
        #print(link)
        try:
            data = requests.get(link, timeout=15)
            soup = BeautifulSoup(data.content, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)  # Extract text content from HTML
            texts.append(text)
        except RequestException:
            texts.append("")
    return texts

def search(query):
    columns = ["query", "rank", "link", "title", "snippet", "html", "created"]
    storage = DBStorage()

    stored_results = storage.query_results(query)
    if stored_results.shape[0] > 0:
        stored_results["created"] = pd.to_datetime(stored_results["created"])
        return stored_results[columns]

    print("No results in database.  Using the API.")
    results = search_api(query)
    html = scrape_page(results["link"])
    results["html"] = html
    results = results[results["html"].str.len() > 0].copy()
    results["query"] = query
    results["created"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    results = results[columns]
    results.apply(lambda x: storage.insert_row(x), axis=1)
    print(f"Inserted {results.shape[0]} records.")
    return results

#def generate_csv(results, filename):
    #with open(filename, mode='w', newline='', encoding='utf-8') as file:
       # writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)

       # writer.writerow(["query", "link", "title", "text", "created"])  # Writing header row
        #for _, row in results.iterrows():
            #try:
               #writer.writerow([row["query"], row["link"], row["title"], row["text"], row["created"]])
            #except Exception as e:
                #print(f"Failed to retrieve HTML for {row['link']}: {e}")
                #continue
#
# Example usage
if __name__ == "__main__":
    query = "Canoo expense structure"
    results = search(query)  # Assuming search() function is defined and returns the DataFrame
    fi = Filter(results)
    filtered = fi.filter()
    print(results)
    #results["text"] = scrape_page(results["link"])  # Adding text content to the DataFrame

    #generate_csv(results, "Revenue_2.csv")
