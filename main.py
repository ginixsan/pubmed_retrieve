from pymongo import MongoClient
import urllib.request
import gzip
import os
import requests
from bs4 import BeautifulSoup
import xmltodict
import re
import argparse


def connect_to_db():
    client = MongoClient('localhost', 27017)
    db = client['pubmed']
    collection = db['ajustes']
    return collection


def get_last_updated_file(collection):
    last_updated_doc = collection.find_one(sort=[("filename", -1)])
    last_updated_file = last_updated_doc['filename']
    return last_updated_file


def get_file_number(file_name):
    return int(re.search('n(\d+)', file_name).group(1))


def download_and_unzip(url, directory):
    file_name = url.split('/')[-1]
    gz_file_name = os.path.join(directory, file_name)
    xml_file_name = gz_file_name.replace('.gz', '')
    urllib.request.urlretrieve(url, gz_file_name)
    with gzip.open(gz_file_name, 'rb') as f_in:
        with open(xml_file_name, 'wb') as f_out:
            f_out.write(f_in.read())
    return xml_file_name


def get_file_links(url):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    links = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith('.xml.gz')]
    return links


def process_files(links, last_number, directory, collection):
    links = [link for link in links if get_file_number(link) > last_number]
    for link in sorted(links):
        xml_file_name = download_and_unzip(link, directory)
        with open(xml_file_name, 'r') as file:
            xml_dict = xmltodict.parse(file.read())
        for pubmed_article in xml_dict['PubmedArticleSet']['PubmedArticle']:
            existing_article = collection.find_one(
                {'PubmedArticle.PubmedData.ArticleIdList': pubmed_article['PubmedData']['ArticleIdList']})
            if existing_article:
                pubmed_article["updated"] = True
                collection.update_one({'_id': existing_article['_id']}, {'$set': pubmed_article})
            else:
                pubmed_article["updated"] = False
                collection.insert_one(pubmed_article)


def update_references(collection):
    for doc in collection.find({'updated': True}):
        for ref in doc['PubmedData']['ReferenceList']:
            referenced_article = collection.find_one(
                {'PubmedArticle.PubmedData.ArticleIdList': ref['Reference']['Citation']['ArticleIdList']['ArticleId']})
            if referenced_article:
                collection.update_one(
                    {'_id': doc['_id'], 'PubmedData.ReferenceList': {'$elemMatch': {
                        'ArticleIdList.ArticleId': ref['Reference']['Citation']['ArticleIdList']['ArticleId']}}},
                    {'$set': {'PubmedData.ReferenceList.$.referenceId': referenced_article['_id']}}
                )


def main(directory):
    collection = connect_to_db()
    last_updated_file = get_last_updated_file(collection)
    last_number = get_file_number(last_updated_file)
    links = get_file_links('https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/')
    process_files(links, last_number, directory, collection)
    update_references(collection)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update PubMed database.')
    parser.add_argument('directory', type=str, help='Directory for downloading files.')
    args = parser.parse_args()
    main(args.directory)
