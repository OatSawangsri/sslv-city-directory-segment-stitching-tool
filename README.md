## sslv-city-directory-segment-stitching-tool

### Sample use case
- RunProcess.py
  - Loop thorough books list
    - create SegmentStitiching obj the target book
    - process

### Interface
#### SegmentStitching(book)
- Ctor
- param 
  - book - target book name
- return 
  - self

### process(summary=None)
- Main function to start the digesting process
- param
  - summary - a filename to write log file to
- return
  - page_processed - number of page processed
  - num_record - number of record write

### Code 
- Ctor()
  - Param
    - bookName
  - create connection
- process()
  - get the page range for the book
  - get all segment in that book
  - get all intersection in that book
  - process page by page 
    - determind each segment if it is a parent or child
    - push result onto output dataframe
    - write log to file if specify
  - write to db

### How it work
- The code process one book at at time
- When process start it will pull the whole book for segment and intersection
- The process will run from the first segment and build a parent-child key pair
- It determine the parent-child relation by detect any delimiter between, intersection
- It compare the OCR box that it extract from to determine the position of the box


