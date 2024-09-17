import os
import shutil

class TextProcessor:
    def merge_txt_files(self, folder_path):
        for root, dirs, files in os.walk(folder_path):
            # Filter out non-text files
            txt_files = sorted([file for file in files if file.endswith('.txt')])

            if txt_files:
                folder_name = os.path.basename(root)

                # Create or open the merged file for writing
                merged_file_name = folder_name + "_merged.txt"
                merged_file_path = os.path.join(root, merged_file_name)
                with open(merged_file_path, 'w') as merged_file:
                    for txt_file in txt_files:
                        # Read the content of each text file and write it to the merged file
                        txt_file_path = os.path.join(root, txt_file)
                        with open(txt_file_path, 'r') as current_file:
                            merged_file.write(current_file.read())
                        os.remove(txt_file_path)

    def find_matching_pdf(self, input_folder, pdf_folder):
        for root, dirs, files in os.walk(input_folder):
            for file_name in files:
                if file_name.endswith(".txt"):
                    file_name = file_name.split('_merged')[0]
                    # Construct the PDF file name by changing the extension
                    pdf_file_name = os.path.splitext(file_name)[0] + ".pdf"

                    # Construct the full paths for the text and PDF files
                    txt_file_path = os.path.join(root, file_name)
                    pdf_file_path = os.path.join(pdf_folder, pdf_file_name)

                    if os.path.exists(pdf_file_path):
                        shutil.copy(pdf_file_path, root)

    def merge_files(self, output_folder_path):
        # Call the method to merge text files
        self.merge_txt_files(output_folder_path)

    def move_pdf(self, input_folder_path, pdf_folder_path):
        # Call the method to find and copy matching PDF files
        self.find_matching_pdf(input_folder_path, pdf_folder_path)