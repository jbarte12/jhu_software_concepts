Name: Jayna Bartel (jbarte12)


Module Info: Module 2 Assignment: Web Scraping
     	     Due on 02/01/2026 at 11:59 EST

Approach:

- Overall: There are two distinct parts to this assignment. The first being the initial scraping
  and cleaning of the data from the grad cafe website to produce applicant_data
  .json. The second being running applicant_data.json through a local LLM and
  producing out.json which has the same data as applicant_data.json with two extra
  columns: llm-generated-program and llm-generated-university. The main files of
  this readme will concentrate on scrape.py, clean.py and main.py which were
  created for the first part.

------PART ONE------
- Scrape.py:
 This script starts by importing the re, urllib.request, and BeautifulSoup
 libraries that will be used within the methods to scrape html data from the
 gradcafe website. The script also imports json for file handling, time for
 tracking how long it takes to scrape data and concurrent
 .features/ThreadPoolExecutor which allows multiple threads to be used at once
 to decrease processing time. The script then goes on to define what I would
 consider the base parameters - the base URL (general link to gradcafe), the
 link to the survey page and its formatting, the maximum number of records we
 want to scrape (30,000), the number of workers (potential detail page requests
  being made), and the HTTP timeout that raises an error if a page doesn't respond for a
 certain amount of time. The file then defines applicant_data.json as the
 output file.

 The script then goes into the methods. As per the homework, private methods
 are defined with an underscore in front.

 The first method _fetch_html is sending a request to the server and does so
 using a user-agent
 header to reduce the chance of the site flagging the requests. The response is
  then read, decoded and returned as a string. This method also contains some
  logic that is triggered if an HTTP request fails that allows the script to
  retry pulling a record three times (waiting longer between pulls each time) if
  it does not go on the first attempt. This was added to reduce the risk of the
 entire program timing out.

 The next method is _clean_text which removes unwanted white spaces with strip
 and split. Once values are split they are a list of separate words so join is
 used to create an evenly spaced string output. The method also standardizes
 empty inputs as empty strings.

 The method _extract_dt_dd is used to search HTML and find the dt elements and
 determine if they match to a label (ex: program, GPA, etc.). If there is a
 matching label the corresponding dd element is also returned. If no matching
 dt label is found an empty string is returned.

 The next method, _extract_undergrad_gpa is used to both find the undergrad GPA
 (labeled Undergrad GPA within the HTML) and normalize the result to an empty
 string if the gpa is given as some type of 0. This was done because quite a
 few entries technically had a GPA, but the value was 0 so I treated it as an
 empty input since it does not provide actual information.

 The method _extract_gre_scores is used to find the gre scores within an
 individual results page. It starts by initializing the GRE fields as a dictionary.
 It then uses soup to find all of the span tags. It then initializes a for loop
 that uses enumerate (allows index and value to be tracked) to iterate over the
 span elements. It starts by extracting the labels and seeing if the end with
 a : because all of the GRE fields within the HTML end with a :. If the labels do
 end with a colon the script first checks to make sure there is a span tag after
 the one being evaluated and if there is it extracts the value within that next
 span tag and maps it to the GRE label tag (ex: gre_verbal is
 the index and the score is the value it is tracking). A series of if an elif
 statements are used to determine which GRE label the detected label matches.
 The scores are also standardized - if a score of 0 appears it was treated as
 an empty field. The information is returned as a dictionary.

 The method _scrape_detail_page starts by creating the detail pages URLs which
 consist of the base url, result and an id. It uses the previously created
 method _fetch_html to retrieve the individual results page and converts it
 into a BeautifulSoup object. The page is parsed to extract detailed information
 based on the _extract_gre_scores method and labels within the HTML - the
 information is then stored in key-value pairs in a dictionary that is returned.

 The _parse_survey_page method is used to parse the results on the main survey
 page. It starts by turning html data into a BeautifulSoup object. It then
 finds  the table rows (tr). The rows are then checked for data
 (td) and checked to determine if they are the main result row. If they are,
 the result link is extracted, if not, the row is skipped. The result ID is
 then extracted a dictionary containing key-value pairs of relevant information
 is initialized. The method then continues to scan rows and rows metadata are processed
 by searching for the div tag which is where information such as start term and
 international status live.


 The scrape_data method starts by setting a timer to track how long it takes to
 scrape the data. It then initializes a list to store the data and a set to
 determine if the IDs are unique. It also starts a page counter. A while loop
 is initialized and set to run until the max number of records is achieved. The
 methods _fetch_html and _parse_survey_page are called upon to retrieve the
 HTML from the main survey page and then parse it. There is a break statement
 that stops the script if no information is found. A for loop
 is then used to iterate over the individual results which are first checked to
 ensure they are unique with an if statement. If the results are unique they
 are added to the list. Another if statement is used to measure how records
 there are and once that value is equal to or greater than the max value the script
 breaks. Once the individual ids are all looked at the next survey page is
 called. ThreadPoolExecutor is
 used here to allow multiple calls to be sent to the website at the same time
 and then merged with record.update. Once the program is done scraping the time
 stops and prints out how long it took. The records are returned.

 The save_data methods creates a json file named applicant_data.json and saves
 data to it in a json format. It also prints out the number of records saved to the file.

- Clean.py:
 This script starts with importing the json and re libraries as well as
 defining both the raw and output file names as applicant_data.json.

 The load_data method uses with open to open a raw data file and returns a json
  object as a python object.

 The _norm method standardizes empty inputs by returning them as empty strings
 and normalizes existing whitespace from strings.

 The _normalize_status method is aimed at cleaning the applicant status. First,
 the script checks if the entry is blank and returns an empty string if so. If
 not, it normalizes the case to lower so everything is easier to work with. It
 then checks if the status has the work wait in it, if it does the status is
 returned as waitlisted. Similarly, if the word interview is in the status the
 status is returned as interview. More data comes with accepted/rejected. If
 the word accepted or rejected is in the status, the decision word is extracted
 using split and strip. The day and month are then located in the string (if
 there is no date the script calls for only the decision to be returned) and
 set to match. The day and month are then extracted from match and returned in
 the format decision: day month.

 The clean_data method starts by initializing an empty list called cleaned. It
 then goes through the raw_records that it accepts as an input and matches
 information in the records to the proper label. During the matching the data
 is cleaned using the _norm and _normalize_status methods. The cleaned data
 is then returned as a list of dictionaries.

 The save_data method opens an output file and produces a json file. It also
 prints out a confirmation
 message that the data was saved to the file.

-Main.py:

 Main starts by importing clean and scrape (described above). It then pulls the
  non-helper methods described
 above (no leading underscore) to process the data in a straight forward format
 . The data is scraped using
 scrape_data (from scrape) and then saved using save_data (from scrape). The
 data is then passed through
 clean_data (from clean) and the cleaned output is finally saved using
 save_data (from clean). The classic
 if __name__ == __main__ is used to ensure the script is only run when executed
  directly.

 This script can be run using the simple run button in conjunction with main.py
 .

------PART TWO------
-App.py
 The data the scraping method produced here already separated the program and
 university names into distinct fields. The LLM app.py code had to be adjusted
 to that the program_text was constructed from
 both the program and university names rather than assuming they are already
 combined. This update was
 made under the standardize and _cli_process_file methods.

 The changes made here did not impact how the code was run, so the readme is
 still valid.

 I added University of Wisconsin - Milwaukee to the Canonical list. I did not
 see any huge issues when
 running the LLM. Some edge cases I noticed were related to the University of
 California system (and
 every university I noticed an issue with was already on the canon_universities
 .txt list). Quite a
 few people would say University of California and put the specific school in
 parentheses (ex: University of California (UCSB)), and the LLM was not consistent about
 removing the parenthesis
 sometimes it would sometimes it wouldn't. I tried playing around with removing
  it but my results
 tended to be negative (it would just simplify to University of California for
 example instead of
 expanding so I left the extra info there. Some universities also have capitals
  in their names -
 I noticed McMaster university on the last pass over which was spelled
 correctly on the input but
 the LLM standardized it to Mcmaster because of standard capitalization rules
 even though that
 university is in the canon_universities list. These notes have been added to
 the LLM readme as well.

Known Bugs: There are no known bugs, however; users may have to adjust the
 num_workers on scrape.py depending on their WiFi networks. The code I pushed
 to GitHub had two workers and was what I used to collect the finalized data
 on a network I know was not very fast. During initial troubleshooting
 the same code I was working with was able to run the same things with 6
 workers on my home network - so just a note that it may be variable, but 2 is
 pretty safe.


