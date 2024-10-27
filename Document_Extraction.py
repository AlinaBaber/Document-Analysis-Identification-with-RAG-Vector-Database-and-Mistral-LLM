import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline, set_seed
from accelerate.utils import release_memory
import time
#meta-llama/Llama-2-13b-chat-hf
class DocumentInformationExtraction:
    def __init__(self, model_name="mistralai/Mistral-7B-Instruct-v0.2"):
        self.tokenizer = None
        self.model = None
        self.text_generation_pipeline = None

        self.initialize_model(model_name)

    def initialize_model(self, model_name):
        if self.text_generation_pipeline is not None:
            # Cleanup existing resources
            self.text_generation_pipeline = None
            self.model = None
            self.tokenizer = None
            torch.cuda.empty_cache()

        self.tokenizer = AutoTokenizer.from_pretrained(model_name, token="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")
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name, device_map='auto', torch_dtype=torch.float16, token="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"
        )
        
        self.text_generation_pipeline = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
            torch_dtype=torch.float16,
            max_new_tokens=4000
        )

    def generate_response(self, file_path, keys_list):
        with open(file_path, 'r') as f:
            context = f.read()
        
        
        instr = f"User will provide you the context (delimited by ```), you need to list down {keys_list}. If any field value doesn't found set that to null. Note that your response will give only above mentioned details."
        prompt = f"""<|system|You are a helpful assistant. Write a response that appropriately completes the request.</s>
{instr}

Context:```{context}```

Generate a response in json format with keys in snake_case.
</s>
"""
        print("Inside Extraction")
        output = self.text_generation_pipeline(prompt)
        print("After Extraction")
        generated_text = output[0]['generated_text'].replace(prompt, '').replace('\n', '')
        
        release_memory(self.text_generation_pipeline)
        time.sleep(2)
        
        

        return generated_text
        from langchain_huggingface import HuggingFacePipeline
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from langchain.vectorstores import Chroma
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_community.document_loaders import PyPDFLoader, Docx2txtLoader, TextLoader
from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
import torch

class RAGPDFAnalyzer:
    def __init__(self, model_name="mistralai/Mistral-7B-Instruct-v0.2"):  # Use a smaller model for compatibility
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForCausalLM.from_pretrained(model_name, torch_dtype=torch.float32)  # Change to float32 for CPU
        self.text_pipeline = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
            max_length=1024,
            temperature=0.5,
            top_p=0.95,
            repetition_penalty=1.15,
            torch_dtype=torch.bfloat16,
            device_map="auto")
        self.llm = HuggingFacePipeline(pipeline=self.text_pipeline)  # Initialize the HuggingFacePipeline here
        self.db = None

    def read_pdf(self, file_path):
        loader = PyPDFLoader(file_path)
        return loader.load()

    def read_docx(self, file_path):
        loader = Docx2txtLoader(file_path)
        return loader.load()

    def read_txt(self, file_path):
        loader = TextLoader(file_path)
        return loader.load()

    def load_db(self, file_path):
        # Determine the file type and extract text
        if file_path.lower().endswith('.pdf'):
            document_text = self.read_pdf(file_path)
        elif file_path.lower().endswith('.docx'):
            document_text = self.read_docx(file_path)
        elif file_path.lower().endswith('.txt'):
            document_text = self.read_txt(file_path)
        else:
            raise ValueError("Unsupported file type. Supported types are: 'pdf', 'docx', 'txt'.")

        # Split the text into manageable chunks
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=1024, chunk_overlap=64)
        self.texts = self.text_splitter.split_documents(document_text)

        # Generate embeddings for the texts
        self.embeddings = HuggingFaceEmbeddings(model_name="hkunlp/instructor-xl", model_kwargs={"device": "cpu"})  # Set to cpu for compatibility
        self.db = Chroma.from_documents(self.texts, self.embeddings)

    def generate_response(self, query):
        prompt = PromptTemplate(template=""" You are a knowledgeable chatbot, responsible for providing detailed and comprehensive answers to queries.
            Your responses should be thorough, clear, and aligned with business analysis standards.
            Always clarify the context if needed.
            Context: {context}
            User: {question}
            chatbot:""", input_variables=["context", "question"])

        qa_chain = RetrievalQA.from_chain_type(
            llm=self.llm,
            chain_type="stuff",
            retriever=self.db.as_retriever(search_kwargs={"k": 2}),
            return_source_documents=True,
            chain_type_kwargs={"prompt": prompt},
        )

        query_result = qa_chain(query)
        response_content = query_result['result']

        return response_content

    def analyze_document(self, file_path, keys_list):
        self.load_db(file_path)
        sections_queries = {
            "questions": f"User will provide you the context (delimited by ```), you need to list down {keys_list}. If any field value doesn't found set that to null. Note that your response will give only above mentioned details."
        }
        project_info = {}

        for section, query in sections_queries.items():
            response = self.generate_response(query)
            print(f"Response for {section}: {response}")

analyzer = RAGPDFAnalyzer()
import os
import fitz

def split_pdf_and_analyze(pdf_path):
  # Create an instance of the DocumentAnalyzer
  analyzer = RAGPDFAnalyzer()

  # Open the PDF
  doc = fitz.open(pdf_path)

  # Iterate through each page in the PDF
  for page_number in range(len(doc)):
      page = doc.load_page(page_number)

      # Create a temporary file path for the single-page PDF
      temp_pdf_path = f'temp_page_{page_number + 1}.pdf'

      # Create a new PDF for the current page
      temp_doc = fitz.open()  # Create a new empty PDF
      temp_doc.insert_page(pno=0)  # Insert a new page at position 0
      temp_doc[0].show_pdf_page(temp_doc[0].rect, doc, page_number)  # Copy the current page
      temp_doc.save(temp_pdf_path)  # Save the single-page PDF
      temp_doc.close()

      # Call the analyze_document method with the temporary file
      analyzer.analyze_document(temp_pdf_path)

      # Optionally, delete the temporary file if not needed
      os.remove(temp_pdf_path)

    # Close the original document
  doc.close()
