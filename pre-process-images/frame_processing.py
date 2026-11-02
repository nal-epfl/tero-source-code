import cv2
import numpy as np


def pre_process_image(image, gray, inverse=True):
    upscaled = cv2.resize(gray, (image.shape[1] * 4, image.shape[0] * 4), interpolation=cv2.INTER_CUBIC)

    kernel = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
    im = cv2.filter2D(upscaled, -1, kernel)

    threshold = cv2.THRESH_BINARY_INV if inverse else cv2.THRESH_BINARY

    ret3, binary = cv2.threshold(im, 0, 255, threshold + cv2.THRESH_OTSU)
    kernel = np.ones((2, 2), np.uint8)

    erosion = cv2.dilate(binary, kernel, iterations=3)
    erosion = cv2.erode(erosion, kernel, iterations=3)
    erosion = cv2.dilate(erosion, kernel, iterations=3)
    erosion = cv2.erode(erosion, kernel, iterations=3)

    return binary


def process_image(subimage):
    new_kernel = np.ones((3, 3), np.uint8)
    resized_sub_image = cv2.resize(subimage, (subimage.shape[1] * 4, subimage.shape[0] * 4),
                                   interpolation=cv2.INTER_CUBIC)
    resized_sub_image = cv2.dilate(resized_sub_image, new_kernel, iterations=3)
    resized_sub_image = cv2.erode(resized_sub_image, new_kernel, iterations=5)
    resized_sub_image = cv2.dilate(resized_sub_image, new_kernel, iterations=3)

    return resized_sub_image
