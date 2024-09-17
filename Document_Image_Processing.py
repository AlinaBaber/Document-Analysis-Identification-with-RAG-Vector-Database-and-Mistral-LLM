import os
import cv2
import numpy as np
from glob import glob
from pdf2image import convert_from_path
import imutils as im

class DocumentImagePreprocessing:

    def __init__(self):
        self.dummy = None

    def pdf_to_images(self, pdf_path, pdf_output_folder):
        # Ensure the output folder exists for PDF with its name
        if not os.path.exists(pdf_output_folder):
            os.makedirs(pdf_output_folder)

        # Open the PDF file
        pdf_document = convert_from_path(pdf_path)#, poppler_path=r"./UTILS/poppler-23.11.0/Library/bin")

        # Get the total number of pages in the PDF
        total_pages = len(pdf_document)

        for page_no in range(total_pages):
            # Save the image to the output folder
#             print('HAHA :',pdf_output_folder)
            prefix= pdf_output_folder.split("/")[-1]
#             print('haha :',prefix)
            image_filename = str(prefix+'_'+f"page_{page_no + 1}.png")  # Naming the images as page_1.png, page_2.png, etc.
            image_path = os.path.join(pdf_output_folder, image_filename)
            pdf_document[page_no].save(image_path, 'PNG')

    def process_multiple_pdfs(self, pdf_folder, output_folder):
        # Create the output folder if it doesn't exist
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # List all files in the PDF folder
        pdf_files = [f for f in os.listdir(pdf_folder) if f.lower().endswith('.pdf') or f.lower().endswith('.PDF')]

        for pdf_file in pdf_files:
            pdf_path = os.path.join(pdf_folder, pdf_file)

            # Create the output folder if it doesn't exist
            pdf_output_folder = os.path.join(output_folder, os.path.splitext(pdf_file)[0])
            if not os.path.exists(pdf_output_folder):
                os.makedirs(pdf_output_folder)

            # Convert the PDF to an image using pdf_to_images function
            self.pdf_to_images(pdf_path, pdf_output_folder)

#     def get_images_paths(self,images_dir):

#         images_paths = glob(os.path.join(images_dir,"*/*.png"))
#         return images_paths
        
    def determine_score(self, arr, angle):
        # data = im.rotate(arr, angle, reshape=False, order=0)
        data = im.rotate(arr, angle)
        histogram = np.sum(data, axis=1, dtype=float)
        score = np.sum((histogram[1:] - histogram[:-1]) ** 2, dtype=float)
        return histogram, score

    def correct_skew(self, image, delta=1, limit=5):
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        thresh = cv2.threshold(image, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
        scores = []
        angles = np.arange(-limit, limit + delta, delta)
        for angle in angles:
            histogram, score = self.determine_score(thresh, angle)
            scores.append(score)
        best_angle = angles[scores.index(max(scores))]
        h, w = image.shape
        center = (w // 2, h // 2)
        M = cv2.getRotationMatrix2D(center, best_angle, 1.0)
        rotated = cv2.warpAffine(image, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
        return best_angle, rotated

    def document_image_rotation(self, image):
        best_angle, rotated_image = self.correct_skew(image)
        return best_angle, rotated_image

    def preprocess_and_resize_image(self, image_path, blur_kernel_size=(5, 5)):
        # Loading the image
        image = cv2.imread(image_path)

        # Applying Gaussian blur
        blurred_image = cv2.GaussianBlur(image, blur_kernel_size, 0)

        # Get the original dimensions of the image
        original_height, original_width = blurred_image.shape[:2]

        # Calculating/Declare scaling factors for width and height
        # target_height, target_width = image.shape
        width_scale = 2  #target_width / original_width
        height_scale = 2 #target_height / original_height

        # Choosing the minimum scaling factor to ensure the entire image fits
        scale = min(width_scale, height_scale)

        # Resizing the image while preserving the aspect ratio
        resized_image = cv2.resize(blurred_image, (int(original_width * scale), int(original_height * scale)))

        return resized_image

    def get_preprocessed_images(self,input_folder, output_folder):

        # Ensure the output folder exists
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            
        pdf_img_folders=os.listdir(input_folder)
        
        for pdf_img_folder in pdf_img_folders:
            output_subfolder=os.path.join(output_folder,pdf_img_folder)
            os.makedirs(output_subfolder,exist_ok=True)
            
            image_paths=glob(os.path.join(input_folder,pdf_img_folder)+'/*.png')
            
            for image_path in image_paths:
                preprocessed_img = self.preprocess_and_resize_image(image_path)
                angle, preprocessed_img = self.document_image_rotation(preprocessed_img)
                
                pre_img_filename = image_path.split("/")[-1].split('.png')[0]+'.jpg'
                cv2.imwrite(os.path.join(output_subfolder,pre_img_filename), preprocessed_img)
