# Document Analysis and identification with RAG Vector Database and Mistral LLM

## Overview

This project is a comprehensive document analysis system, designed to automate the processing and analysis of documents from acquisition to consumption. It integrates advanced machine learning and AI models like **RAG (Retrieval Augmented Generation) Vector Databases** and **Mistral LLM** to efficiently extract, match, enrich, and process document information. The pipeline covers:

- **Document Acquisition**
- **Document OCR (Optical Character Recognition)**
- **Document Preprocessing**
- **Document Information and Title Extraction**
- **Document Matching and Enrichment**
- **Document Consumption and Uploading**

## Features

- **Document Acquisition**: Automated gathering of documents from specified sources.
- **OCR Integration**: Converts scanned images or PDFs into machine-readable text using Optical Character Recognition (OCR).
- **Preprocessing**: Cleans and prepares documents for downstream analysis (removal of noise, handling of incomplete documents).
- **Information Extraction**: Title and relevant document information extraction using **RAG vector database** and **Mistral LLM**.
- **Document Matching & Enrichment**: Leverages AI models to match and enrich documents with metadata and other contextual information.
- **Consumption & Uploading**: Final processed document output, ready for consumption, and automatically uploaded to a specified location.

## Architecture

1. **Document Acquisition**:
   - Connects to a variety of data sources (local, cloud storage, web scraping).
   
2. **Document OCR**:
   - Uses popular OCR libraries (e.g., Tesseract) for converting images or scanned PDFs into editable text.
   
3. **Preprocessing**:
   - Text cleaning, noise reduction, normalization, and segmentation.
   
4. **Information Extraction**:
   - Uses **RAG** and **Mistral LLM** for extracting meaningful information like document title, summary, and other details.
   
5. **Matching and Enrichment**:
   - Matches documents with existing records and enriches them with metadata using contextual AI models.
   
6. **Document Uploading**:
   - Final step to upload or store the enriched documents in a specified repository or database.

## Getting Started

### Prerequisites

- Python 3.x
- Libraries: `pandas`, `numpy`, `spacy`, `torch`, `transformers`, `tesseract`, `opencv`, `sentence-transformers`
- Access to a RAG-compatible Vector Database
- **Mistral LLM** API Key

### Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/username/Document-Analysis-Pipeline-LLM-RAG.git
    ```

2. Navigate to the project directory:

    ```bash
    cd Document-Analysis-Pipeline-LLM-RAG
    ```

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

### Usage

1. **Configure Sources:**
   - Define the document sources for acquisition (local storage, cloud storage, etc.).
   
2. **Run the Pipeline**:

    ```bash
    python main.py --source [path_to_documents] --output [output_path]
    ```

3. **OCR & Preprocessing**:
   - The system will automatically run OCR on documents and clean the text.

4. **Information Extraction**:
   - Extract document titles and key information using the integrated **Mistral LLM** and **RAG** models.

5. **Enrichment & Matching**:
   - The processed documents will be enriched with additional metadata and matched to existing datasets if applicable.

6. **Final Output**:
   - The final output is ready for upload or further analysis.

### Configuration

- **Document Sources**: Define paths to documents in `config.json`.
- **Vector Database**: Set up the connection details for the **RAG Vector Database** in `rag_config.yaml`.
- **Mistral LLM**: Provide API keys for **Mistral LLM** integration in `ml_config.yaml`.

### Contributor

### License

This project is licensed under the MIT License.
