Name: Jayna Bartel (jbarte12)


Module Info: Module 3 Assignment: Database Queries Assignment Experiment
     	     Due on 02/08/2026 at 11:59 EST

Approach:

- Overall:
This assignment uses run,py to start a flask app that runs a website containing
information about the data that was scraped during module 2. The overall
approach is to use run.py to start the applicant - python run.py in the
terminal will launch the app. The app itself contains information parsed
from the scraped database using SQl queries. The app also has two buttons:
pull data and update analysis. Pull data depends on the script refresh_gradcafe
 and scrapes grad_cafe for information not currently in the database. Update
 analysis runs that scraped data through the LLM and then udpates the webpage
 with the new/updated stats.

The code is heavily commented. I will give an overview of the approach here.

Please note I will not be explaining the models and scrape folders since they
are pulled directly from module_2 and contain the LLM and scrape files. The app
folder also contains significant duplications from module one so I will only be
explaining the HTML related to the page and the pages.py file.

------App Folder------

- Overview: This fold runs very similar to how the app folder in module_1 ran.
It has static and templates folders accompanied by an __init__.py and pages.py
file. The main updates here are the pages.py and gradcafe_stats.html so I will
talk to those.

- gradcafe_stats.html: This file is an extension of the base.html file and
controls the basic formatting of the display page for this assignment. The page
itself contains data that was scraped from gradcafe and parsed using SQL queries. It
contains a header: GradCafe Application Statistics and displays multiple
dynamic stats.It also contains buttons to update the page by pulling more data and
incorporating it into the displayed stats.

- pages.py: The script starts by importing relevant modules and scripts. It
then defines a file as pull_state.json which is a file that will be used to
determine if the webpage is actively thinking during dynamic updates. It also
defines a flask blueprint. The first method read_state opens the pull_state
.json and reads it. The second method write_state writes the pull_state.json
file and contains contents their either state that data is still being pulled
or that the pull is complete. The script then defines the route grad_cafe which
queries data in PostgreSQL and reads the refresh state of the application
which will be used to determines if the buttons can be active or not. The next
methodrefresh_gradcafe starts a scrape of new gradcafe data and keeps the flask app
running. It tracks the pull state to prevent duplicate scraps and control if
the buttons are enabled on the screen (if pull data is going update analysis
cannot).The next route update_analysis runs the LLM on the newly scraped data,
inserts it into the database and requeries the data with the new information.
It prevents updates during an active pull and displays a message once the
update is done.

------ Module 3 Folder------

- Overview: The module_3 folder is where the meat of the information for
handling this assignment is found. Here we have

- load_data.py: This file starts by importing relevant modules. It then goes on
to create the create_connection method that defines the database name, user,
password, host and PostgreSQL port. It uses a try/except method to try and
open a connection or print an error message if the connection is unsuccessful.
The next method execute_query sends an SQL command to the database and applies
changes immediately. The rebuild_from_llm_file rebuilds the database table,
reads data line by line from the llm_extend_applicant_data.json and cleans and
converts the field into the correct formats. It then bulk inserts all data into
the database and skips entries that would duplicate an existing URL (this
should not happen with the way the code is written for scraping but serves as a
backup). The sync_db_From_llm_field method is next and does essentially the
same thing as the previous method but only to new data. It will read a .json
file, clean/convert the data and insert it into a database without overwriting
existing files. This is useful for when we want to update the database with new
entries but don't want to have to process data that has already been processed.

- query_data.py: The script starts by importing relvent modules. It then goes
on to define a method called fetch_value which runs an SQL query, takes the
first returned row and returns a single value from the first column (or None)
. The next method fetch_row does the same thing but returns an entire row
instead of a column. The next method get_application_stats defines all
SQL queries. It starts by connecting to the database. It then goes on to make
an number of SQl queries. The first being total applicants which uses COUNT to
count every row in the grad_applicant table and return that as the total number
of applicants in the database. The next is the Fall 2026 applicant count which
uses COUNT to once again count the rows, but also uses the WHERE clause to
filter rows before counting them - so only rows that contain term - Fall 2026
are counted. The next query is an international applicant count that again uses
COUNT and WHERE to filter. Here it is filtering on the word international and
uses the LOWER so things are no case-sensitive. The average GPA, GRE, GRE
Verbal and GRE AW all use SELECT to find the specific data and AVG to average
out the data. Once again FROM is used to say we are grabbing it from the
grad_applications table. The average GPA for US applicants in Fall of 2026 is
found using SELECT to hone in on GPA, FROM to specify the table, WHERE to
filter the term, LOWER to make it not case-sensitive as well as a clause that
says we are only looking for results that have included a GPA. Fall 2025 total
applicants simply uses COUNT to again count rows and WHERE to filter on the
term. The accepted applicants for fall of 2025 does the same thing but adds in
LIKE to look for columns that have statuses that start with accepted. The
average GPA for Fall 2026 accepted students uses AVG to find the average, WHERE
 to filter and LIKE to again look for applicants that have a status starting
 with accepted. There is also a clause that the GPA is not null. The query
 about JHU CS masters students uses COUNT to count rows, WHERE to filter on
 master degrees, and LIKE to find strings and an equal sign to find exact
 matches. This query also has quite a bit of error sanitizing and includes
 different ways people could indicate CS or spell/misspell JHU. The methods to
 find the CS, PhD students at MIT, Stanford, Georgetown, and CMU uses the same
 methods. The final two methods are looking for the percentage of applicants
 who included their GPA and were either accepted or rejected in Fall 2026.
 These methods use COALESCE wrapped around their functions to return 0 if there
 are no rows to compute. They then use FILTER to allow for multiple conditions
 to be applied and WHERE to stipulate that the GPA is not Null and is greater
 than 0. They then search for the term Fall 2026 and filter for statuses that
 have rejected or accepted. The method then has a series of print statements
 and returns the stats as a dictionary.

 - refresh grad_cafe: The script starts by importing relevant modules. The
 method get_seen_ids_from_llm_extend_file reads the previously saved records,
 extracts the IDs and adds them to a set. The scrape_new_records starts
 scraping results from the initial survey page and continues scraping until it
 sees 3 already captured IDs in a row on a survey page. As it scrapes it adds
 the new records to a list that is returned. The enrich_with_details method
 then takes those IDs and goes through and parses data from the individual
 results pages.The write_new_applicant_file appends the new data to a file
 named new_applicant_data.json which will be used for processing. The refresh
 method runs the methods above and scrapes the data and writes them to a clean
 staging file and reports how many records were scraped. If there are no new
 records it exits the process

 - update_data.py: This scripts starts by importing relevant modules including
 the LLM. The script then goes on to define update_data which starts by
 defining two files: new_applicant_data.json and llm_extend_applicant_data.json
 . The script then uses try to attempt to open and load the new_applicant_data
 .json file and except to handle errors if it is not found as well as if the
 file is empty. It defines a variable processed that starts the initial count
 of how many records are processed. The new data is then processed. It starts
 by combining program and university into one record, it then calls the LLM,
 processes the data and adds it to the final llm_extend_applicant_data.json
 file. Before looping up it increases the processed variable to track the
 number of records. Once the data is processed the new_applicant_data is
 overwritten to be empty which prevents the update_analysis button the website
 from pulling data that has already been pulled.

 - run.py: This is a simple script that imports the app, and uses create_app to
  start the flask instance. This file starts the webpage by inputting python
  run.py into the terminal.
