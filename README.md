# Document Processing

This repository contains scripts for extracting text from documents, generating a term-document matrix, and storing the processed data in a MongoDB collection. It is designed to facilitate text analysis, entity recognition, and knowledge graph construction.

The corpus is divided into 11 categories and each category of document is processed separately.

## Transcripts

1. Extracting Speaker and Texts
The Speaker names and their corresponding Texts are extracted from the transcripts, and arranged / categorized by 2 year intervals.

2. Term Document Matrix - Combined and Exact Text
This script is used to create the Term Document Matrix object for the Combined Text and Exact column in the mongodb collection. This term document matrix will be used to perform the first search, where we will get the speaker rankings for the keywords. The keywords in search query can be input in three ways:

- Single Keyword (dairy)
- Mulitple Comma Separated Keywords (dairy, agriculture, sustainability)
- Multi-worded phrases (food wise, dairy milk)

## Publications

1. Extracting Speaker and Texts 
The texts are extracted from the publications.

2. Term Document Matrix
This script is used to create the Term Document Matrix object for the texts extracted in the mongodb collection. This term document matrix will be used to perform the search and filter the set of relevant documents.
