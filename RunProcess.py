from SegmentStitching import SegmentStitching


t_book_list_1 = [
  "gaatlantasub1975hain",
  "gaatlantasub1981hain",
  "gaatlanta1966haines",
  "gaatlantasub1986hain",
  "gaatlantasub1991hain",
  "gaatlantacity1970haines",
  "gaatlantacity1991haines",
  "gaatlantacityan1967haines",
  "gaatlantasuburba1970haine",
]

t_book_list_2 = [
  "gaatlantasuburban1984polk",
  "gaatlantasuburban1988polk",
  "gaatlantasuburban1965atla",
  "gaatlantasuburban1969atla",
  "gaatlantasuburban1974atla",
  "gaatlantasuburban1979atla",
  "gaatlanta1940atlantacityd",
  "gaatlanta1986polkdirector",
  "gaatlanta1991polkdirectoryco",
]

t_book_list_3 = [
  "gaatlantacounty1953atlantacitydire",
  "gaatlantacounty1956atlantacitydire",
  "gaatlantacounty1960atlantacitydire",
  "gaatlantasub1960atlantacitydire",
  "gaatlantasub1966atlantacitydire",
  "gaatlantasub1970atlantacitydire",
  "gaatlantasub1982atlantacitydire",
  "gaatlantasub1975atlantacitydire"
]


for book in t_book_list_1:
  file_name = './log/' + book + '_summary.csv'

  print("= START process: " + book + "  == ")

  ss_a = SegmentStitching(book)
  page_processed, num_records = ss_a.process(summary=file_name)

  print("------  processed:   " + str(page_processed) + " pages with   " + str(num_records) + " records --")
  print("====== FINISHED process: " + book + "  ==========")




