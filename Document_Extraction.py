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
