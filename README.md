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

### Table Create 
```
CREATE TABLE CityDirDev.dbo.CdSTD_street_segement_parent_lookup_v2(
	ParentID numeric(38,0) NULL,
	ChildID numeric(38,0) NOT NULL
);
CREATE INDEX CdSTD_street_segement_parent_lookup_ParentID_IDX ON CityDirDev.dbo.CdSTD_street_segement_parent_lookup (ParentID);
ALTER TABLE CityDirDev.dbo.CdSTD_street_segement_parent_lookup ADD CONSTRAINT CdSTD_street_segement_parent_lookup_PK PRIMARY KEY (ChildID);
```

## Note
- Mulit page segment
  - http://dcssearchuat:3002/bookKeys/gaatlantasub1975hain/images/86

### Need to check
- total record - 335540
- record with higher than 5 column distinct - 59086
```
select count(*) FROM CdSTD_street_segement_parent_lookup cssspl where ChildID >= ParentID + 5
```

- does not have intersction -- have street -- 
  - gaatlantasuburba1970haine
  - gaatlanta1966haines

- need to look at street "tag" (blue) - city (orange)
- testing query 
  - randome pick where parent and child is different by the degree of 10
  - queyr for the whole page
```
select * from CityDir.dbo.CdStreetSegments css 
inner join(select top 1 * from CdSTD_street_segement_parent_lookup cssspl where ChildID >= ParentID + 10 ORDER BY NEWID()) ssp
on ID in (ssp.ParentID,	ssp.ChildID) 

select * from CityDir.dbo.CdStreetSegments css where ImageKey = 92 and BookKey = 'gaatlantasuburba1970haine'
```


### Sublisting Enhancment