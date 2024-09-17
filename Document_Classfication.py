import os
import pandas as pd
import tabulate
from Document_Object_Detection import DocumentObjectDetection
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from Document_Object_Detection import DocumentObjectDetection

class TextClassifier:
    def __init__(self, category_file_path, input_folder_path, output_folder_path):
#       self.object_detection = DocumentObjectDetection(detection_label='Payment')
        self.category_data = pd.read_excel(category_file_path)
        #self.category_names = self.category_data['Sub docs'].tolist()
        self.filtered_category_data = self.category_data[self.category_data['STATUS'] == 'CLEAR']
        # Assuming 'Documents' is the column you want to extract values from
        self.category_names = self.filtered_category_data['Documents'].tolist()
        #self.category_names = self.category_data['Documents'].tolist()
        
        self.category_accuracy_counts = {category: {"correct": 0, "total": 0} for category in self.category_names}
        self.input_folder_path = input_folder_path
        self.output_folder_path = output_folder_path

        if not os.path.exists(self.output_folder_path):
            os.makedirs(self.output_folder_path)

    def predict_category(self, text, column_values):
        vectorizer = CountVectorizer()
        # Combine the input text and the category values into a list for vectorization
        all_texts = [text] + column_values
        vectors = vectorizer.fit_transform(all_texts)
        
        # Calculate cosine similarity between the input text and each category value
        similarities = cosine_similarity(vectors[0], vectors[1:])[0]
        
        # Get the index of the category with the highest similarity score
        max_similarity_index = similarities.argmax()

        # If the highest similarity score is above a certain threshold, return the corresponding category
        if similarities[max_similarity_index] > 0.8:
            return column_values[max_similarity_index]
        else:
            return ""

    def process_text_files(self):
        for root, dirs, files in os.walk(self.input_folder_path):
            for file_name in files:
                if file_name.endswith(".txt"):
                    file_path = os.path.join(root, file_name)
                    with open(file_path, 'r', encoding='utf-8') as file:
                        input_text = file.read()

                    predicted_documents = self.predict_category(input_text, self.filtered_category_data['Documents'].tolist())

                    if not predicted_documents:
                        for value in self.filtered_category_data['Documents'].tolist():
                            if isinstance(value, str) and value.lower() in input_text.lower():
                                predicted_documents = value
                                break

                    if predicted_documents:
                        matching_record = self.filtered_category_data[self.filtered_category_data['Documents'] == predicted_documents]
                        print(matching_record)

                        if not matching_record.empty:
                            predicted_main_docs = matching_record['Process'].values[0] if 'Process' in matching_record.columns else ""
                            predicted_sub_docs = matching_record['Documents'].values[0] if 'Documents' in matching_record.columns else ""
                            alternative = matching_record['Alternative'].values[0]
                            print('Alternative:', alternative)
                            
                            if str(alternative)=='nan':
                                pass
                            else:
                                predicted_sub_docs = alternative
                                
                            
                            print('Object Detection : ',matching_record['Object Detection'].values[0])
                            to_detect = matching_record['Object Detection'].values[0]
                            true_category = matching_record['Documents'].values[0]
                            
                            
                            print("Matching Record:")                      
                            selected_columns_json = matching_record.iloc[:, :6].to_json(orient='records', lines=True)
                            print(selected_columns_json)

#                             #print(tabulate.tabulate(matching_record.iloc[:, :6], headers='keys', tablefmt='pretty'))  

                            main_docs_folder_path = os.path.join(self.output_folder_path, predicted_main_docs)
                            sub_docs_folder_path = os.path.join(main_docs_folder_path, predicted_sub_docs)
        
                            if to_detect=='None':
                                
                                for folder in [main_docs_folder_path, sub_docs_folder_path]:
                                    if not os.path.exists(folder):
                                        os.makedirs(folder)

                                output_file_path = os.path.join(sub_docs_folder_path, file_name)
                                with open(output_file_path, 'w', encoding='utf-8') as output_file:
                                    output_file.write(input_text)

                                print(f"File '{file_name}' classified and saved to '{output_file_path}'")

                                self.category_accuracy_counts[true_category]["total"] += 1
                                if true_category == predicted_documents:
                                    self.category_accuracy_counts[true_category]["correct"] += 1
                            
                            if to_detect in ['Signature','Date','Payment']:
                                
                                doc= DocumentObjectDetection(detection_label=to_detect,pred_cat = true_category)
                                
                                
                                output_file_path = os.path.join(sub_docs_folder_path, file_name)
                                pdf_name = os.path.basename(output_file_path).split('_merged')[0]
                                images_folder = os.path.join('OutputOfDocPipeline','Pdfs2Images',pdf_name)
                                
                                obj_det_predicted_sub_docs = doc.get_detection(input_folder=images_folder)
                                
                                print('(%)',obj_det_predicted_sub_docs)
                                
                                main_docs_folder_path = os.path.join(self.output_folder_path, predicted_main_docs)
                                sub_docs_folder_path = os.path.join(main_docs_folder_path, obj_det_predicted_sub_docs)
                                
                                for folder in [main_docs_folder_path, sub_docs_folder_path]:
                                    if not os.path.exists(folder):
                                        os.makedirs(folder)
                                        
                                new_output_file_path=os.path.join(sub_docs_folder_path,file_name)

                                with open(new_output_file_path, 'w', encoding='utf-8') as output_file:
                                    output_file.write(input_text)

                                print(f"File '{file_name}' classified and saved to '{new_output_file_path}'")                               

                        else:
                            print(f"No matching record found in the Excel sheet for document type: {predicted_documents}")
                            self.save_to_others_folder(file_name, input_text)

                    else:
                        print("No matching category found in the 'Sub docs' column for the given text in file:", file_path)
                        self.save_to_others_folder(file_name, input_text)
                        
#         return to_detect

    def save_to_others_folder(self, file_name, input_text):
        others_folder_path = os.path.join(self.output_folder_path, "others")
        if not os.path.exists(others_folder_path):
            os.makedirs(others_folder_path)

        output_file_path = os.path.join(others_folder_path, file_name)
        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            output_file.write(input_text)

        print(f"File '{file_name}' classified as 'others' and saved to '{output_file_path}'")

    def display_accuracy_counts(self):
        print("\nAccuracy Counts:")
        for category, counts in self.category_accuracy_counts.items():
            print(f"{category}: Correct: {counts['correct']}, Total: {counts['total']}")
