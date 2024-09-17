import os
import difflib
import pytesseract
from glob import glob
from paddleocr import PaddleOCR

class DocumentOCR:
    def __init__(self):
        self.dummy = None
        self.paddle_ocr = PaddleOCR(lang='en',use_gpu=True)

    def get_pytesseract_ocr(self, image_path):
        custom_config1 = r'--oem 1 --psm 3'
        
        try:
            return pytesseract.image_to_string(image_path ,output_type='string', config=custom_config1, lang='eng')
        
        except Exception as e:
            print("Error during PyTesseract OCR:", str(e))
            return ""

    def get_paddleocr_ocr(self, image_path):

        try:
            return self.paddle_ocr.ocr(image_path)

        except Exception as e:
            print("Error during PaddleOCR OCR:", str(e))
            return ""
        
    def perform_ocr(self, input_folder, output_folder, mode='hybrid'):
        image_folders = os.listdir(input_folder)

        for image_folder in image_folders:
            if 'pytesseract' in mode.lower() or 'hybrid' in mode.lower():
                # For Pytesseract Folder
                pyt_ocr_folder = os.path.join(output_folder, 'PYTESSERACT', image_folder)
                os.makedirs(pyt_ocr_folder, exist_ok=True)

            if 'paddleocr' in mode.lower() or 'hybrid' in mode.lower():
                # For PaddleOCR Folder
                paddleocr_ocr_folder = os.path.join(output_folder, 'PADDLEOCR', image_folder)
                os.makedirs(paddleocr_ocr_folder, exist_ok=True)

            image_paths = glob(os.path.join(input_folder, image_folder, '*.jpg'))
            
            print(image_folder)

            for image_path in image_paths:
                file_name = image_path.split('/')[-1][:-4]
                
                if "page_1." in image_path or "page_2." in image_path or "page_3." in image_path:

                    if 'pytesseract' in mode.lower() or 'hybrid' in mode.lower():
                        # For Pytesseract OCR Text
                        pyt_ocr_txt = self.get_pytesseract_ocr(image_path)
                        text_file1 = open(os.path.join(pyt_ocr_folder, f"{file_name}_pytesseract.txt"), "w", encoding='utf-8')
                        text_file1.write(pyt_ocr_txt)
                        text_file1.close()
                        print("Pytesseract OCR Generated!")

                    if 'paddleocr' in mode.lower() or 'hybrid' in mode.lower():
                        # For PaddleOCR OCR Text
                        paddle_ocr_list = self.get_paddleocr_ocr(image_path)
                        paddle_ocr_txt = [x[1][0] for x in paddle_ocr_list[0]]
                        text_file2 = open(os.path.join(paddleocr_ocr_folder, f"{file_name}_paddleocr.txt"), "w", encoding='utf-8')
                        for text in paddle_ocr_txt:
                            text_file2.write(f"{text}\n")
                        text_file2.close()
                        print("PaddleOCR OCR Generated!")
                else:
                    continue;

                
    def read_files(self, file_path):
        """
        Read the contents of a file and return the content as a string.

        Parameters:
        - file_path (str): The path to the file.

        Returns:
        - str: The content of the file.
        """
        with open(file_path, 'r') as file:
            content = file.read()
            return content

    def hybrid_txt(self, file1_path, file2_path):
        """
        Perform hybrid text processing on two files.

        Parameters:
        - file1_path (str): The path to the first file.
        - file2_path (str): The path to the second file.

        Returns:
        - str: The processed text.
        """
        # Read the contents of each file
        file1_content = self.read_files(file1_path)
        file2_content = self.read_files(file2_path)

        final_txt = file1_content

        # Split the contents of each file into words
        words_file1 = set(file1_content.split())
        words_file2 = set(file2_content.split())

        # Find common words
        common_words = words_file1.intersection(words_file2)

        # Find unique words in each file
        unique_words_file1 = words_file1 - common_words
        unique_words_file2 = words_file2 - common_words

        # Iterate through unique words and perform replacement based on similarity
        for word in unique_words_file1:
            for other_word in unique_words_file2:
                # Skip words containing digits
                if any(char.isdigit() for char in word) or any(char.isdigit() for char in other_word):
                    continue

                # Calculate similarity ratio between words
                seq = difflib.SequenceMatcher(None, word, other_word)
                diff_ratio = seq.ratio() * 100

                # If similarity ratio is above 75%, perform replacement
                if diff_ratio >= 75.0:
                    if len(other_word) >= len(word):
                        final_txt = final_txt.replace(word, other_word)

        return final_txt

    def get_hybridized_result(self, input_dir, output_dir, mode="hybrid"):
        """
        Generate hybridized results for OCR output files.

        Parameters:
        - input_dir (str): The input directory containing OCR output files.
        - output_dir (str): The output directory to store hybridized results.
        - mode (str): The mode of operation (currently supports 'hybrid').

        Returns:
        - None
        """
        if mode == 'hybrid':
            pytesseract_root = os.path.join(input_dir, 'PYTESSERACT')
            paddle_root = os.path.join(input_dir, 'PADDLEOCR')
            
            result_folder = os.path.join(output_dir, input_dir.split('/')[-1])
            os.makedirs(result_folder, exist_ok=True)

            for pytesseract_folder in os.listdir(pytesseract_root):
                if not os.path.isdir(os.path.join(pytesseract_root, pytesseract_folder)):
                    print(f"ERROR : {os.path.isdir(os.path.join(pytesseract_root, pytesseract_folder))} => --DOES NOT EXIST--")
                    continue

                folder1_path = os.path.join(pytesseract_root, pytesseract_folder)
                folder2_path = os.path.join(paddle_root, pytesseract_folder)

                file1_paths = sorted(glob(os.path.join(folder1_path, '*.txt')))
                file2_paths = sorted(glob(os.path.join(folder2_path, '*.txt')))
                
                for file1_path in file1_paths:
                    corresponding_file2_path = os.path.join(folder2_path, os.path.basename(file1_path)).replace('pytesseract','paddleocr')
                    if corresponding_file2_path in file2_paths:
                        # Read the contents of each file
                        file1_content = self.read_files(file1_path)
                        file2_content = self.read_files(corresponding_file2_path)

                        # Find common, unique, and new words
                        h_text = self.hybrid_txt(corresponding_file2_path,file1_path)

                        text_folder = os.path.join(result_folder, pytesseract_folder)
                        os.makedirs(text_folder, exist_ok=True)
                        txt_name = f"{os.path.basename(file1_path).split('.txt')[0]}"+"_hybrid.txt"
                        text_file_path = os.path.join(text_folder, txt_name).replace('_pytesseract','')
                        text_file = open(text_file_path, "w")
                        text_file.write(h_text)
                        text_file.close()

        if mode=='pytesseract':
            pass
        
        if mode=='paddleocr':
            pass