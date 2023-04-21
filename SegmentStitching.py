import pyodbc
import pandas as pd
import numpy as np
from scipy import stats
import os

from utils.database.DatabaseFactory import DatabaseFactory


STREET_SEGMENT_TABLE = "CityDir.dbo.CdStreetSegments"
INTERSECTION_TABLE = "CityDir.dbo.CdIntersections"


class SegmentStitching:

    def __init__(self, book):
        self.conn = self.connect()
        self.book = book


        #self.current_delimiter = None
        #self.page_range= self.get_page_range()
        #self.current_page_info = self.all_page_col.iloc[0].to_dict() 
        #self.current_page_info = {'ImageKey': 1595, 'minCol': 1, 'maxCol': 5}
        self.whole_page_intersects = None
        

    def connect(self):
        server = 'labdatadev-db.dev.corp.lightboxre.com' 
        database = 'CityDir' 
        write_db = 'CityDirDev'
        user = os.environ.get("CD_DB_USER")
        pswrd = os.environ.get("CD_DB_PASS")

        self.db_factory = DatabaseFactory(default_type='mssql', default_host=server, default_database=database, default_username=user, default_password=pswrd)

        return self.db_factory.create_connection().connection

    def get_page_range(self):
        query = """
            select
                MIN(ImageKey) as minKey,
                max(ImageKey) as maxKey
                from {}
            where
                BookKey = ?
        """.format(STREET_SEGMENT_TABLE)
        params = [self.book]
        df = pd.read_sql(query, self.conn, params=params)
        df['minKey'] = df['minKey'].astype(int)
        df['maxKey'] = df['maxKey'].astype(int)
        
        return df.iloc[0].to_dict()

    def get_page(self, page):
        query = '''
            select
                ID, ImageKey , ImageColumn, ListingsExtent, StreetText, CityText
            from
                {}
            where
                ImageKey = ?
                and BookKey = ?
                and ListingsExtent is not null
            ORDER BY ID ASC
        '''.format(STREET_SEGMENT_TABLE)
        params = [str(page), self.book]

        return pd.read_sql(query, self.conn, params=params)

    def get_intersect_page(self, page):
        query = '''
            select 
                ID, ImageKey , ImageColumn, CrossText, CrossExtent 
            from 
                {} ci 
            WHERE 
                BookKey = ?
                and ImageKey = ?
            ORDER BY ID ASC
        '''.format(INTERSECTION_TABLE)

        params = [self.book, str(page)]

        return pd.read_sql(query, self.conn, params=params)

    def get_intersection_info(self, col):
        return self.whole_page_intersects[self.whole_page_intersects["ImageColumn"] == col]

    def is_something_below(self, in_segment_info, in_col):
        # check if somethiing below
        intersect_info = self.get_intersection_info(int(in_col))
        
        if(len(intersect_info) == 0):
            return False;

        inx_coord = eval(intersect_info.iloc[-1].CrossExtent)
        seg_coord = eval(in_segment_info.ListingsExtent)

        
        if(self.compare_coord_higher(inx_coord,seg_coord)):
            return True
        return False
    
    def is_something_above(self, in_segment_info, in_col):
        # check if somethiing below
        intersect_info = self.get_intersection_info(int(in_col))
        
        if(len(intersect_info) == 0):
            return False;

        inx_coord = eval(intersect_info.iloc[0].CrossExtent)
        seg_coord = eval(in_segment_info.ListingsExtent)

        if(self.compare_coord_higher(seg_coord,inx_coord)):
            return True
        return False

    def compare_coord_higher(self, seg_coord, inx_coord):
        # statement = is seg_coord hihger than inx_coord
        # coord struct = [x1, y1, x2, y2]
        # only need to check the first y coord
        if(seg_coord[1] > inx_coord[1]):
            return True
        return False

    def build_delimiter_collections(self,page):
        # add as function so we can add in delimiter later
        self.whole_page_intersects = self.get_intersect_page(page)
        return 

    def process(self, in_pages=None, in_prior_seg=None):
        pages = None

        if(in_pages):
            pages = in_pages
        else:
            pages = self.get_page_range()
            pass


        # for segment that is prior to the current page (only use for test)
        prior_segment = in_prior_seg
        for page in range(pages["minKey"], pages["maxKey"] + 1):
            outputList, prior_segment = self._process_page(page, prior_segment)
            print(outputList)
            print("==============================")


    def _process_page(self, page, prior_segment=None):

        output = []
        parent_seg = None
        #start at frist page
        whole_page_segments = self.get_page(page)
        if(whole_page_segments.empty):
            return -1

        # build a collection of table to use as delimiter in stitching segment
        self.build_delimiter_collections(page)

        temp = None
        for segment in whole_page_segments.iterrows():
            temp = None
            seg_obj = segment[1]
            seg_id = segment[1]["ID"]
            seg_col = segment[1]["ImageColumn"]
            # First one to run, No prior query
            if(prior_segment is None):
                # this is the parent - prior segment have ended
                parent_seg = seg_id
                temp = {"parent": parent_seg, "child": parent_seg}
                if(self.is_something_below(seg_obj, seg_col)):
                    prior_segment = None
                else:
                    prior_segment = seg_id

            # go to next one
            else:
                if(self.is_something_above(seg_obj, seg_col)):
                    # this is the parent - if something above ( at the begining of the column)
                    parent_seg = seg_id
                    temp = {"parent": parent_seg, "child": parent_seg}
                    if(self.is_something_below(seg_obj, seg_col)):
                        prior_segment = None
                    else:
                        prior_segment = seg_id
                else:
                    # this is a child
                    parent_seg = prior_segment
                    temp = {"parent": parent_seg, "child": seg_id}
                    if(self.is_something_below(seg_obj, seg_col)):
                        prior_segment = None

            output.append(temp)
        # end For loop
        return output, prior_segment

    def writeKeyPair(self, value):
        # write to csv for now
        pass


    def testCurrent(self):
        print("-----Current----------------------------")
        self.process_page()
        print("current_page_info" + str(self.current_page_info))


    def testState(self):
        print("-----General----------------------------")
        print("page_list:")
        print(self.all_page_col)
        
        

