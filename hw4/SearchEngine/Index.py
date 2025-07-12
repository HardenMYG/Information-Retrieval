# 这个文件用于从webpages.csv文件中读取一个每一个html文件并解析title, content, anchor,
# 并将数据存入索引web_pages中

import csv
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup

# Initialize Elasticsearch client
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# Define the index settings and mappings
index_settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "url": {"type": "keyword"},
            "title": {"type": "text"},
            "content": {"type": "text"},
            "anchors": {
                "type": "nested",
                "properties": {
                    "anchor_text": {"type": "text"},
                    "target_url": {"type": "keyword"},
                },
            },
        }
    },
}

# Create the index
index_name = "web_pages"
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
es.indices.create(index=index_name, body=index_settings)


import os
from urllib.parse import urljoin, quote, urlparse
import chardet


# Function to extract data from HTML
def extract_data_from_html(url, html_path):
    with open(html_path, "rb") as file:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        encoding = result["encoding"]

    with open(
        html_path.replace("\\", "/"), "r", encoding=encoding, errors="ignore"
    ) as file:
        soup = BeautifulSoup(file, "lxml")
        title = (
            soup.title.string.strip().replace("\n", "").replace(" ", "")
            if soup.title and soup.title.string
            else ""
        )
        content = ",".join(
            [
                line.strip().replace("\n", "").replace(" ", "")
                for line in soup.get_text().splitlines()
                if line.strip()
            ]
        )
        anchors = []
        for a in soup.find_all("a"):
            anchor_text = a.get_text().strip().replace("\n", "").replace(" ", "")
            try:
                href = a.get("href", "")
                if not urlparse(href).netloc:
                    target_url = urljoin(url, quote(href))
                else:
                    target_url = href
                anchors.append({"anchor_text": anchor_text, "target_url": target_url})
            except ValueError as e:
                print(f"Skipping invalid URL {href}: {e}")
        return title, content, anchors


# Read the CSV file and index the documents
csv_file_path = "D:\\SearchEngine\\webpages.csv"
actions = []
i = 1
max_file_size = 10 * 1024 * 1024  # 10 MB

with open(csv_file_path, "r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    i = 0
    for row in reader:
        url = row["URL"]
        html_path = row["Filename"]

        # Check file size
        if os.path.getsize(html_path) > max_file_size:
            print(f"Skipping {html_path} due to large file size.")
            continue

        title, content, anchors = extract_data_from_html(url, html_path)

        action = {
            "_index": index_name,
            "_source": {
                "url": url,
                "title": title,
                "content": content,
                "anchors": anchors,
            },
        }

        # 打印所有获取到的数据
        # print(f"URL: {url}")
        # print(f"Title: {title}")
        # print(f"Content: {content[:100]}...")  # Print only the first 100 characters of content for brevity
        # print("Anchors:")
        # for anchor in anchor_texts:
        #     print(f"  Anchor Text: {anchor['anchor_text']}, Target URL: {anchor['target_url']}")

        actions.append(action)
        i += 1
        # if i ==10:
        #     break
        if i % 100 == 0:
            print(f"Processed {i} documents.")

# Bulk index the documents
print("Indexing...")
helpers.bulk(es, actions, chunk_size=100, request_timeout=120)
print("Done!")