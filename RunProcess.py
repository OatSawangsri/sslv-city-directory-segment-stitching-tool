from SegmentStitching import SegmentStitching


t_book_list_2 = [
  "gaatlantasub1975hain",
  "gaatlantasub1981hain",
  "gaatlantasub1982atlantacitydire",
  "gaatlantasub1986hain",
  "gaatlantasub1991hain",
  "gaatlantasuburba1970haine",
  "gaatlantasuburban1965atla",
  "gaatlantasuburban1969atla",
  "gaatlantasuburban1974atla",
  "gaatlantasuburban1979atla",
  "gaatlantasuburban1984polk",
  "gaatlantasuburban1988polk",
  "gaatlantasub1975atlantacitydire"
]

t_book_done = [
  "gaatlanta1940atlantacityd",
  "gaatlanta1966haines",
  "gaatlanta1986polkdirector",
  "gaatlanta1991polkdirectoryco",
  "gaatlantacity1970haines",
  "gaatlantacity1991haines",
]

t_book_list_1 = [
  "gaatlantacityan1967haines",
  "gaatlantacounty1953atlantacitydire",
  "gaatlantacounty1956atlantacitydire",
  "gaatlantacounty1960atlantacitydire",
  "gaatlantasub1960atlantacitydire",
  "gaatlantasub1966atlantacitydire",
  "gaatlantasub1970atlantacitydire",
]

t_book_list_ex = [
    
]

for book in t_book_list_2:
  file_name = './log/' + book + '_summary.csv'

  print("= START process: " + book + "  == ")

  ss_a = SegmentStitching(book)
  page_processed, num_records = ss_a.process(summary=file_name)

  print("------  processed:   " + str(page_processed) + " pages with   " + str(num_records) + " records --")
  print("====== FINISHED process: " + book + "  ==========")




