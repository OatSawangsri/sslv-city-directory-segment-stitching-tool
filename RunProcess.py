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

t_book_list_4 = [
  "gaatlantacounty1953atlantacitydire",
]


tx_book_list_1 = [
  "txlubbock1990polkdirector",
  "txlubbock1980polkdirector",
  "txlubbock1975polkdirector",
  "txlubbock1970hudspethdire",
  "txlubbock1966hudspethdire",
  "txlubbock1963hudspethdire",
  "txlubbock1952hudspethdire",
  "txlubbock1947hudspethdire",
  "txlubbock1943hudspethdire",
  "txlubbock1940hudspethdire",
  "txlubbock1931hudspethdire"
]

tx_book_list_2 = [
  "txlubbock1958hudspethdirecto",
  "txlubbock1955hudspethdirecto",
  "txlubbock1935hudspethdirecto",
  "txlubbock1926hudspethdirecto",
  "txlubbock1989cole",
  "txlubbock1985cole",
  "txlubbock1980cole",
  "txlubbock1975cole"
]

ca_book_list_1 = [
  "casanbernardino1991haines",
  "casanbernardino1986haines",
  "caredlands1972generaltele",
  "caredlands1967luskeybroth",
  "caredlands1965luskeybroth",
  "caredlands1950jessiepcisco",
  "caredlands1947arthurcommercia",
  "caredlands1936losangelesdirec",
  "caredlands1931losangelesdirec",
  "caredlands1927losangelesdirec",
  "caredlands1923losangelesdirec",
  "caredlands1919losangelesdirec"
]


for book in ca_book_list_1:
  file_name = './log/' + book + '_summary.csv'

  print("= START process: " + book + "  == ")

  ss_a = SegmentStitching(book)
  page_processed, num_records = ss_a.process(summary=file_name)

  print("------  processed:   " + str(page_processed) + " pages with   " + str(num_records) + " records --")
  print("====== FINISHED process: " + book + "  ==========")




