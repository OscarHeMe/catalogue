# -*- coding: utf-8 -*-
from config import *
from ByHelpers import applogger
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import cv2
import boto3
import ast
import time
from ByRequests.ByRequests import ByRequest

br = ByRequest(timeout=15)
headers_kroger = {'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36"}
br.add_proxy(OXYLABS, attempts=3)
logger = applogger.get_logger()
PLOT_IMG = False
MAX_RETRIES = 3
IMG_EXPIRATION = 15  # Days

# Logger
logger = applogger.get_logger()

class ImageProduct(object):
    @staticmethod
    def download_img(prod, retries=0):
        """ Download product images, and stores it in
            local directory, it uses cache when available.

            Params:
            -----
            prod : dict
                Product info

            Returns:
            -----
            choosed_img : dict
                Selected image dict with product_uuid, image, and content(np.ndarray)
        """
        if isinstance(prod['images'], list):
            _imgs = prod['images']
        else:
            logger.warning('Unable to parse image URL!')
            logger.debug(prod['images'])
            return None
        _im_objs = []
        # Download images
        for _im in _imgs:
            logger.debug('Fetching: {}'.format(_im))
            try:
                if 'kroger' in _im:
                    img_content = br.get(_im, headers=headers_kroger)
                else:
                    img_content = br.get(_im)
                if img_content:
                    img_content = img_content.content
                else:
                    raise ("Cannot obtain img_response")
                if img_content:
                    _blob = cv2.imdecode(
                        np.asarray(
                            bytearray(img_content),
                            dtype=np.uint8
                        ), -1)
                    if _blob is None:
                        continue
                    _im_objs.append({
                        'product_uuid': prod['product_uuid'],
                        'image': _im,
                        'content': _blob
                    })
                else:
                    raise ("Cannot obtain img_content")
            except requests.exceptions.SSLError:
                logger.warning("Wrong SSL connection with: {}".format(_im))
            except requests.exceptions.Timeout:
                # Wait for some time and retry
                logger.warning("Retrying to get ImageProduct...")
                time.sleep(10)
                # If max retries continue
                if retries < MAX_RETRIES:
                    return ImageProduct.download_img(prod, retries=retries + 1)
            except Exception as e:
                logger.error(e)
                logger.warning(_im)
                # Verify image existance
        if not _im_objs:
            logger.debug('Could not find any downloadable image!')
            return None

        # Choose best image
        choosed_img = sorted([(_b['content'].shape[0] * _b['content'].shape[1], _b) \
                              for _b in _im_objs], key=lambda x: x[0], reverse=True)

        images_sorted = [image[1] for image in choosed_img]
        # Upload files to s3
        try:
            ImageProduct.upload_s3(images_sorted)
        except Exception as e:
            logger.error(e)
            logger.warning("Could not update Bucket in Category")

        return images_sorted

    @staticmethod
    def upload_s3(images):
        """ Upload images to S3 Product bucket

            Params:
            -----
            images : list
                List of Image attributes (PUUID, content, etc.)
        """
        try:
            # AWS S3 session
            session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
            )
            if not session:
                return None
            # Configure product Bucket
            s3 = session.resource('s3')
            bucket = s3.Bucket(AWS_PRODUCT_BUCKET)
            # Delete all existant images in product path
            for b in bucket.objects.filter(Prefix="{}/".format(images[0]['product_uuid'])):
                try:
                    b.delete()
                except Exception as e:
                    logger.debug(e)
                    pass
            logger.debug("Deleted S3 old files..")
            # Upload all files
            _uplded = 0
            for k, i in enumerate(images):
                pth = 'products/{}/{}.png'.format(i['product_uuid'], k)
                try:
                    image_io = cv2.imencode('.png', i['content'])[1].tobytes()
                    bucket.put_object(Key=pth, Body=image_io, ContentType='image/png')
                    _uplded += 1
                except Exception as e:
                    logger.error(e)
                    pass
            logger.info("Uploaded to S3,  {} new files!".format(_uplded))
        except Exception as e:
            logger.error("Error uploading image to s3: {}".format(str(e)))
        return True

    @staticmethod
    def generate_features(Image, nclusters=50, resize=[2.0, 2.0], DistBorder=10.0, NoDescriptor=1000):
        """ Processing Images to Generate Features

            Params:
            -----
            Image : np.ndarray
                OpenCV Image Object

            Returns:
            -----
            Rep :  np.array
                Increased Array of Figures with features.
        """
        logger.debug("Genetating img features...")
        # Background Removal
        try:
            ImageNoBack1, SigContour1 = ImageProduct.BackGroundSubstractor(Image)
        except Exception as e:
            logger.error(e)
            logger.warning("Not able to substract background")
            return np.array([[]])
        gray_image1 = cv2.cvtColor(Image, cv2.COLOR_BGR2GRAY)
        # Filtering
        img_bilateral1 = cv2.bilateralFilter(gray_image1, 9, 75, 75)
        # Edge Enhancing
        laplacian1 = cv2.Laplacian(gray_image1, cv2.CV_8U)
        EdgeImage1 = cv2.addWeighted(img_bilateral1, 0.5,
                                     laplacian1, 1.5, 0.0)
        logger.debug("Done filtering, now generating Descriptors...")
        # print('Contour size', len(SigContour1))
        # Descriptor Generation
        orb = cv2.ORB_create(nfeatures=NoDescriptor, scoreType=cv2.ORB_FAST_SCORE)
        kps = orb.detect(EdgeImage1, None)
        logger.debug("Detected Descriptors...")
        # Remove Spurious Descriptors
        nkps = []
        for i in range(len(kps)):
            try:
                dist = np.abs(cv2 \
                              .pointPolygonTest(SigContour1[0], kps[i].pt, True))
                if dist > DistBorder:
                    nkps.append(kps[i])
            except IndexError:
                # In case no contours take all KPs
                logger.error("Issues with Index!")
                nkps.append(kps[i])
        logger.debug("Removed Spurious Descriptors...")
        # Get the descriptors inside of the object
        _, Desc = orb.compute(EdgeImage1, nkps)
        logger.debug("Computed Descriptors...")
        if Desc is None:
            return np.array([[]])
        # Generate a list of representations
        _, magic_number = Desc.shape
        logger.debug("Number Descriptors %i" % len(nkps))
        if PLOT_IMG:
            edge_image = cv2.drawKeypoints(EdgeImage1, nkps, None)
            plt.figure()
            plt.imshow(edge_image, cmap='gray')
            plt.show(block=False)
            input('Press [Enter] to contine ...')
            plt.close('all')
        if len(nkps) == 0:  # Case no descriptors where found
            return np.array([[]])
        if len(nkps) < nclusters:  # Case one not enough for clustering
            ADesc2 = np.zeros([nclusters - len(nkps), magic_number])
            if len(kps) > 0:
                ADesc1 = np.asarray(Desc)
                Rep = np.concatenate((ADesc1, ADesc2), axis=0)
            else:
                Rep = ADesc2
        else:  # Enough for clustering
            # Get KMeans
            Code = KMeans(n_clusters=nclusters)
            ADesc3 = np.asarray(Desc)
            Code.fit(ADesc3)
            # Get Labels of points and Center of Clusters
            Labels = Code.labels_
            Centers = Code.cluster_centers_
            LCode = []
            # Use the Sum(v-c) for representative code book
            for i in range(nclusters):
                NN = ADesc3[Labels == i, :]
                LCode.append(np.sum(NN - Centers[i, :], axis=0))
            Rep = np.stack(LCode, axis=0)
        # Get info for the flattening
        n, m = Rep.shape
        # Flatten and Return
        return np.reshape(Rep, (1, n * m))

    @staticmethod
    def BackGroundSubstractor(Image):
        """ Algorithm for Background Removal

            Params:
            -----
            Image : np.ndarray
                OpenCV Image object

            Returns:
            -----
            RImage, SigContour : cv2.Image, cv2.Contour
                No background image and most significant contour
        """
        # Filter for edge detection
        blurred = cv2.GaussianBlur(Image, (5, 5), 0)
        # The Smoothed Image is using B G R -
        EdgeOfImage = np.max(np.array([ImageProduct.DetectEdge(blurred[:, :, 0]),
                                       ImageProduct.DetectEdge(blurred[:, :, 1]),
                                       ImageProduct.DetectEdge(blurred[:, :, 2])]),
                             axis=0)

        # Given the high degree of noise remove by using the mean
        mean = np.mean(EdgeOfImage)
        # Zero any value that is less than mean.
        EdgeOfImage[EdgeOfImage <= mean] = 0
        # Now find the largest Countour
        EdgeOfImageU8 = np.array(EdgeOfImage, np.uint8)  # Convert to gray scale
        SigContour = ImageProduct.SignificantContours(Image, EdgeOfImageU8)  # Use it to find the contours
        # Mask Generation
        mask = EdgeOfImage.copy()
        mask[mask > 0] = 0
        cv2.fillPoly(mask, SigContour, 255)
        # Invert mask
        mask = np.logical_not(mask)
        # Finally remove the background
        RImage = Image.copy()
        RImage[mask] = 0
        return RImage, SigContour

    @staticmethod
    def DetectEdge(channel):
        """ Detection of edges per channel in RGB

            Params:
            -----
            channel : np.array
                Array (matrix) of channel of color

            Returns:
            -----
            sobel : np.array
                Image Array with applied sobel mask

            `Note`:
            -----
            Using Soble Masks
                        [+1 0 -1 ]
            X_Direction = [+2 0 -2 ]
                        [+1 0 -1 ]
                        [+1 +2 +1 ]
            Y_Direction = [ 0 0  0  ]
                        [-1 -2 -1 ]
        """
        # Here we are using a signed depth for each channel 0-255
        # 1,0 It is the X_Direction Edge
        sobelX = cv2.Sobel(channel, cv2.CV_16S, 1, 0)
        # 0,1 It is the Y_Direction Edge
        sobelY = cv2.Sobel(channel, cv2.CV_16S, 0, 1)
        # Equivalent to sqrt(x1**2 + x2**2), element-wise.
        sobel = np.hypot(sobelX, sobelY)
        # Threshold if values are larger than 255
        # Be carful integer values
        sobel[sobel > 255] = 255
        return sobel

    @staticmethod
    def SignificantContours(Image, EdgeOfImage, PercentageImage=10.0):
        """ Find the Largest Contour

            Params:
            -----
            Image : np.ndarray
                OpenCV Image Object
            EdgeOfImage : np.array
                Image Edges
            PercentageImage : float, optional, default=10.0
                Percentage allowed for contours

            Returns:
            -----
            significant : list
                List of Contours

            `Note`:
            -----
            cv.RETR_TREE = retrieves all of the contours and
                            reconstructs a full hierarchy of nested contours.

            cv2.CHAIN_APPROX_SIMPLE = compresses horizontal, vertical,
                                    and diagonal segments and leaves only
                                    their end points.
        """
        image, contours, heirarchy = cv2.findContours(EdgeOfImage,
                                                      cv2.RETR_TREE,
                                                      cv2.CHAIN_APPROX_SIMPLE)
        # Find level 1 contours
        level1 = []
        for i, tupl in enumerate(heirarchy[0]):
            # Each array is in format (Next, Prev, First child, Parent)
            # Filter the ones without parent
            if tupl[3] == -1:
                tupl = np.insert(tupl, 0, [i])
                level1.append(tupl)

        # Find the contours with large surface area.
        significant = []
        # Remove Contours with not at least PercentageImage of the area
        tooSmall = EdgeOfImage.size * PercentageImage / 100.0
        # No contours
        if len(level1) == 0:
            return []
        largest_cont, largest_area = level1[0], 0.0
        for tupl in level1:
            # Get the root of the list
            contour = contours[tupl[0]]
            # Get the contour area
            area = cv2.contourArea(contour)
            # print('Contour Area:', area, ', Too Small:', tooSmall)
            if area > tooSmall:
                significant.append([contour, area])
            # Save the largest
            if area > largest_area:
                largest_cont = contour
                largest_area = area
        # If Significant empty, add largest in contour
        if len(significant) == 0:
            significant.append([largest_cont, largest_area])
        # Sort by key
        significant.sort(key=lambda x: x[1])
        return [x[0] for x in significant]



    @staticmethod
    def compound_vector(vec):
        """ Method to verify type of data sent from descriptor

            Params:
            -----
            vec : str | list
                Image descriptor

            Returns:
            -----
            desc : np.array
                Reconstructed vector
        """
        if isinstance(vec, str):
            return np.array(ast.literal_eval(vec))
        else:
            return np.array(vec)

    @staticmethod
    def find_img_neighbors(im_tree, _mid, img_docs, X_im):
        """ Query KD-Tree for decoded neighbors with
            respective similarities through images.

            Params:
            -----
            im_tree : scipy.spatial.cKDTree
                Spatial KD-Tree
            _mid : dict
                Missing product to query
            img_docs : list
                List of Ordered UUIDs
            X_im : np.array
                Dense Representation of the Image vectors

            Returns:
            -----
            img_neighs : pd.DataFrame
                DF of prod_uuid, with several metrics
        """
        # Empty response
        img_neighs = pd.DataFrame({
            'img_result': [],
            'result_id': [],
            'img_cosine': [],
            'img_query': []
        })
        # Elements validation
        if 'prod_images' not in _mid:
            logger.warning("Missing prod_images field")
            return img_neighs
        elif not _mid['prod_images']:
            logger.warning("Empty prod_images field")
            return img_neighs
        # Empty initialization
        _desc, _imgname = None, None
        for _pim in _mid['prod_images']:
            if 'descriptor' in _pim:
                if _pim['descriptor']:
                    _desc = ImageProduct.compound_vector(_pim['descriptor'])
                    _imgname = _pim['image']
        if _desc is None:
            logger.warning("No descriptor in prod_images")
            return img_neighs
        logger.debug("Computing image KD-Tree distances..")
        # Query distance
        xdistance, iindex = Query_Distance(im_tree, _desc, kelements=10)
        ResultQuery = []
        for ii in iindex[0]:
            ResultQuery.append(img_docs[ii])
        # Results' dense representation
        result_dense = X_im[iindex[0], :]
        # Compute metrics vectors from closest neighbors
        logger.info("Obtaining metrics...")
        cosine_metric = Cosine_Measure_Dense(_desc, result_dense)
        # Append results
        img_neighs = pd.DataFrame({
            'img_result': [y[1] for y in ResultQuery],
            'result_id': [y[0] for y in ResultQuery],
            'img_cosine': pd.Series(cosine_metric)
        })
        img_neighs['img_query'] = _imgname
        img_neighs['result_id'] = img_neighs.result_id.astype(str)
        # logger.debug('\n {}'.format(_imgname))
        # logger.debug('\n %s', img_neighs[['result','cosine']])
        return img_neighs