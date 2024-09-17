import os
import cv2
from glob import glob
from torch import hub

class DocumentObjectDetection:
    def __init__(self, detection_label, pred_cat):
        self.pred_cat = pred_cat

        self.model_path = {'payment': 'models/eob_payment_zero_33.pt',
                           'date': 'models/pres_date_model.pt',
                           'signature': 'models/signature_model_v6.pt'}

        self.is_target = detection_label.lower()

        self.model = hub.load('ultralytics/yolov5', 'custom', path=self.model_path[self.is_target])

    def detection(self, image_path):
        img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
        data = {}
        results = self.model(img, size=416)
        results.show()

        crops = results.crop(save=False)

        res = [sub['im'] for sub in crops]
        label_and_conf = [sub['label'] for sub in crops]
        conf = []
        label = []

        for i in range(len(label_and_conf)):
            labelz, confz = (label_and_conf[i].split(' '))[0], (label_and_conf[i].split(' '))[1]
            label.append(labelz)
            conf.append(confz)

        return label, conf

    def get_detection(self, input_folder):
        images_paths = sorted(glob(os.path.join(input_folder, '*.png')))

        detected_labels = []

        for image_path in images_paths:
            label, confidence = self.detection(image_path)
#             print(f'Image Name : {image_path}')
#             print(f'Labels     : {label}')
#             print(f'Confidence : {confidence}')

            if len(label) > 0:
                detected_labels.extend(label)

        if len(detected_labels) == 0:
            return self.pred_cat
        else:
            if self.is_target == 'payment':
                if 'eob_payment' in detected_labels:
                    return 'EOB Payment'
                else:
                    return 'EOB Zero'

            elif self.is_target == 'date':
                if 'handwritten_date' in detected_labels:
                    return 'Hand Written Prescription'
                else:
                    return 'Prescription'

            elif self.is_target == 'signature':
                if 'signature' in detected_labels:
                    return self.pred_cat
                else:
                    return 'Other Docs'