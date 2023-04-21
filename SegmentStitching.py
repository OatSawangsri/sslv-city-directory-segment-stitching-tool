import pyodbc
import pandas as pd
import numpy as np
from scipy import stats
import os

from utils.database.DatabaseFactory import DatabaseFactory


STREET_SEGMENT_TABLE = "CityDir.dbo.CdStreetSegments"
INTERSECTION_TABLE = "CityDir.dbo.CdIntersections"

ABOVE = 0
BELOW = -1


class SegmentStitching:

    def __init__(self, book):
        
        self.book = book
        self.whole_page_intersects = None
        self.conn = self.connect()

    def connect(self):
        server = 'labdatadev-db.dev.corp.lightboxre.com' 
        database = 'CityDir' 
        write_db = 'CityDirDev'
        user = os.environ.get("CD_DB_USER")
        pswrd = os.environ.get("CD_DB_PASS")

        self.db_factory = DatabaseFactory(default_type='mssql', default_host=server, default_database=database, default_username=user, default_password=pswrd)

        # return an acutal connection not the factory 
        return self.db_factory.create_connection().connection

    # DB retrieve
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

    # process intersection
    def is_something(self, in_segment_info,in_col, location):
        intersect_info = self.whole_page_intersects[self.whole_page_intersects["ImageColumn"] == in_col]
        
        if(len(intersect_info) == 0):
            return False;

        inx_coord = eval(intersect_info.iloc[location].CrossExtent)
        seg_coord = eval(in_segment_info.ListingsExtent)

        if(location == ABOVE):
            if(self.compare_coord_higher(seg_coord,inx_coord)):
                return True
        else:
            if(self.compare_coord_higher(inx_coord,seg_coord)):
                return True
        return False

    def is_something_below(self, in_segment_info, in_col):
        # check if somethiing below
        return self.is_something(in_segment_info, in_col, BELOW)
    
    def is_something_above(self, in_segment_info, in_col):
        # check if somethiing below
        return self.is_something(in_segment_info, in_col, ABOVE)

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

    # process command
    def process(self, in_pages=None, summary=False):
        pages = None
        child_page_list = {}

        if(in_pages):
            pages = in_pages
        else:
            pages = self.get_page_range()
            pass

        # prior_segment - value if parent is in previou page
        prior_segment = None
        for page in range(pages["minKey"], pages["maxKey"] + 1):

            # process one page at a time 
            output_df, prior_segment = self._process_page(page, prior_segment)

            # write to db
            self.write_df_db(output_df)

            # write out summary to csv when require
            if(summary):
                # bookkey , imagekye(page number), number of segement in page, number of child in page
                num_child = len(output_df[output_df['ParentID'] != output_df['ChildID']])
                print(self.book + ","+  str(page) + ","+  str(len(output_df)) + ","+  str(num_child))

        return child_page_list

    def _process_page(self, page, prior_segment=None):

        df_out = pd.DataFrame(columns=['ParentID', 'ChildID'])
        parent_seg = None
        #start at frist page
        whole_page_segments = self.get_page(page)
        if(whole_page_segments.empty):
            return -1

        # build a collection of table to use as delimiter in stitching segment
        self.build_delimiter_collections(page)

        temp = None
        for segment in whole_page_segments.iterrows():
            #print(segment)

            temp = None
            seg_obj = segment[1]
            seg_id = segment[1]["ID"]
            seg_col = segment[1]["ImageColumn"]
            # First one to run, No prior query
            if(prior_segment is None):
                # this is the parent - prior segment have ended
                parent_seg = seg_id
                df_out.loc[len(df_out)] = {'ParentID':parent_seg, 'ChildID': parent_seg }
                if(self.is_something_below(seg_obj, seg_col)):
                    prior_segment = None
                else:
                    prior_segment = seg_obj

            # go to next one
            else:
                if(self.is_something_above(seg_obj, seg_col)):
                    # this is the parent - if something above ( at the begining of the column)
                    parent_seg = seg_id
                    df_out.loc[len(df_out)] = { 'ParentID':parent_seg, 'ChildID': parent_seg }
                    if(self.is_something_below(seg_obj, seg_col)):
                        prior_segment = None
                    else:
                        prior_segment = seg_obj
                else:
                    # this is a child
                    parent_seg = prior_segment["ID"]
                    df_out.loc[len(df_out)] = { 'ParentID':parent_seg, 'ChildID': seg_id}
                    if(self.is_something_below(seg_obj, seg_col)):
                        prior_segment = None
        # end For loop
        return df_out, prior_segment


    def write_df_db(self, df=None):
        self.db_factory.write_df(df, "CityDirDev", "CdSTD_street_segement_parent_lookup ")
        # write to csv for now
        pass
