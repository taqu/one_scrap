from ast import Try
import logging
import os
import os.path
import re
import string
import requests
import json
import hashlib

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

    def __init__(self, site:string, rule:string, db_host:string, db_port:int, chunk_size:int, chunk_overlap:int, collection:string, embedding_address:string):
        super().__init__()
        if site:
            self.sitemap_urls = [site]
        if rule:
            self.sitemap_rules = [(rule,'parse')]
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        self.chroma_client = chromadb.HttpClient(host=db_host,port=db_port)
        self.collection = self.chroma_client.get_or_create_collection(name=collection)
        self.embedding_url = embedding_address + '/v1/embeddings'

    def parse(self, response):
        document = ''
        for line in response.xpath('//body//text()').extract():
            line = line.strip()
            if not line:
                continue
            document += line + '\n'
        chunks = self.text_splitter.split_text(document)
        headers = {'Content-Type': 'application/json'}

        count = 0
        for chunk in chunks:

            chunk_id = hashlib.sha1(('{0}{1:08}'.format(response.url, count)).encode()).hexdigest()
            count = count + 1
            payload = {
                'input': chunk,
                'model': 'multilingual-e5-large',
                'encoding_format': 'float'}
            response = requests.post(self.embedding_url, headers=headers, json=payload)
            if 200 != response.status_code:
                continue
            embedding = response.json()['data'][0]['embedding']
            ids = [chunk_id]
            embeddings = [embedding]
            metadatas = [{'url': response.url}]
            documents = [chunk]
            chroma_response = self.collection.upsert(ids=ids,embeddings=embeddings,documents=documents,metadatas=metadatas)

def run_scrapy(site:string, rule:string, db_host:string, db_port:int, chunk_size:int, chunk_overlap:int, collection:string, embedding_address:string):
    if not site:
        return

    process = CrawlerProcess()
    process.crawl(SiteSpider, site=site, rule=rule,db_host=db_host,db_port=db_port,chunk_size=chunk_size,chunk_overlap=chunk_overlap,collection=collection,embedding_address=embedding_address)
    process.start()

def main():
    sg.theme('DarkAmber')
    layout = [[sg.Text('Site'), sg.InputText(default_text='https://docs.unrealengine.com/sitemapindex.xml')],
              [sg.Text('Rule'), sg.InputText(default_text='/5.3/')],
              [sg.Text('Vector DB Host'), sg.InputText(default_text='localhost')],
              [sg.Text('Vector DB Port'), sg.InputText(default_text='8000')],
              [sg.Text('Chunk Size'), sg.InputText(default_text='480')],
              [sg.Text('Chunk Overlap'), sg.InputText(default_text='48')],
              [sg.Text('Collection'), sg.InputText(default_text='unreal_doc')],
              [sg.Text('Embedding Address'), sg.InputText(default_text='http://localhost:8090')],
              [sg.Button('Run')]]

    window = sg.Window('Window Title', layout)
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            break
        if event == 'Run':
            run_scrapy(values[0],values[1], values[2], int(values[3]), int(values[4]), int(values[5]), values[6], values[7])
            continue

    window.close()

if __name__ == '__main__':
    main()
