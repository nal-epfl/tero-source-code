## Image processing module: pre-process images

### Scripts
1. __pre_process_images__: Takes a set of thumbnails, cuts them around the area where the network data is expected to appear, converts the image to black-and-white, applies several filters and erosion/dilation steps to improve OCR performance. Then, the scripts compresses all images into two zip files: one including the cut image without pre-procesing and a second with the BW images.


### Configuration parameters and secrets:
1. __batch_size__: Number of images to process in one batch.
2. __number_cores__: Number of parallel processes to create.
3. __long_term_storage__: Path to backup storage for the zips.
4. __increase_batch_threshold__: Number of _batch_size__ stored before duplicating the processing rate.
5. __extra_areas__: List of areas of interest to keep but not process.


### Data files:
1. __data/areas_of_interest.json__: Coordinates (in pixels) of the network data in each game thumbnail. Format:
```
 "Tero-generated game ID": {
    "name": "Game name",
    "1080": [{
      "y1": Top corner,
      "y2": Bottom corner,
      "x1": Left corner,
      "x2": Right corner
    }]
  },
```


### Redis configuration:
1. __raw_images__: List of thumbnails to process.