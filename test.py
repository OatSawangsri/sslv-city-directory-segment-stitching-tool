from SegmentStitching import SegmentStitching


book = 'gaatlanta1966haines'
page = {'minKey': 201, 'maxKey': 202}
file_name = './log/' + book + '_summary.csv'
print("= START process: " + book + "  == ")

ss_a = SegmentStitching(book)
page_processed, num_records = ss_a.process(in_page = page, summary=file_name, write=False)

#pages["minKey"], pages["maxKey"] 