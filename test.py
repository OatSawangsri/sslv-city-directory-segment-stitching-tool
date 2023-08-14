from SegmentStitching import SegmentStitching


# book = 'gaatlanta1966haines'
# page = {'minKey': 201, 'maxKey': 202}
# file_name = './log/' + book + '_summary.csv'

# book = 'txlubbock1980cole'
# page = {'minKey': 145, 'maxKey': 146}
# file_name = './log/' + book + '_summary.csv'

book = 'gaatlantasub1975hain'
page = {'minKey': 101, 'maxKey': 102}
file_name = './log/aaa_' + book + '_summary.csv'
print("= START process: " + book + "  == ")

ss_a = SegmentStitching(book)
page_processed, num_records = ss_a.process(in_page = page, summary=file_name, write=False)

#pages["minKey"], pages["maxKey"] 