from ast import Try
import logging
import os
import os.path
import re
import string
import requests
import json

from scrapy.spiders import SitemapSpider
from scrapy.exceptions import CloseSpider
from scrapy.crawler import CrawlerProcess
from langchain.text_splitter import RecursiveCharacterTextSplitter

import PySimpleGUI as sg
import chromadb

class SiteSpider(SitemapSpider):
    name = 'sitespider'
    sitemap_urls = []
    sitemap_rules = [('','parse')]
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'LOG_LEVEL': 'WARNING',
    }

    def __init__(self,site:string,rule:string,chunk_size:int, chunk_overlap:int, collection:string):
        super().__init__()
        if site:
            self.sitemap_urls = [site]
        if rule:
            self.sitemap_rules = [(rule,'parse')]
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        self.chroma_client = chromadb.HttpClient(port='8090')
        self.collection = self.chroma_client.get_or_create_collection(name=collection)

    def parse(self, response):
        document = ''
        for line in response.xpath('//body//text()').extract():
            line = line.strip()
            if not line:
                continue
            document += line + '\n'
        chunks = self.text_splitter.split_text(document)
        url = 'http://localhost:8000/v1/embeddings'
        headers = {'Content-Type': 'application/json'}
        for chunk in chunks:
            payload = {
                'input': chunk,
                'model': 'multilingual-e5-large',
                'encoding_format': 'float'}
            response = requests.post(url, headers=headers, json=payload)
            if 200 != response.status_code:
                continue
            embedding = response.json()['data'][0]['embedding']
            request = {
                'embeddings': [embedding],
                'metadatas': [{'url': response.url}],
                'documents': [chunk],
            }
            chroma_response = self.collection.upsert(request)

def run_scrapy(site:string, rule:string, chunk_size:int, chunk_overlap:int, collection:string):
    if not site:
        return

    process = CrawlerProcess()
    process.crawl(SiteSpider, site=site, rule=rule,chunk_size=chunk_size,chunk_overlap=chunk_overlap,collection=collection)
    process.start()

def main():
    sg.theme('DarkAmber')
    layout = [[sg.Text('Site'), sg.InputText(default_text='https://docs.unrealengine.com/sitemapindex.xml')],
              [sg.Text('Rule'), sg.InputText(default_text='/5.3/')],
              [sg.Text('Chunk Size'), sg.InputText(default_text='480')],
              [sg.Text('Chunk Overlap'), sg.InputText(default_text='48')],
              [sg.Text('Collection'), sg.InputText(default_text='unreal_doc')],
              [sg.Button('Run')]]

    window = sg.Window('Window Title', layout)
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            break
        if event == 'Run':
            run_scrapy(values[0],values[1], int(values[2]), int(values[3]), values[4])
            continue

    window.close()

if __name__ == '__main__':
    main()
