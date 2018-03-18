Project Description: This Project fetches specific information from news database containing (articles, authors, logs) tables
as part of Udacity Full Stack Web Developer Nanodegree.

Dependencies/Pre-requisites: This project needs psycopg2 module with python 3

Setup/Installation: pip install psycopg2

Usage: 
1. Download the git repository using 'git clone https://github.com/vamshimaringanti/LogsAnalysisProject.git'
2. cd LogsAnalysisProject
3. Connect to "news" database from psql using 
    psql -d "news"
4. Create view date_status_code using the following command:
    create or replace view date_status_code as
    select to_char(time, 'FMMonth DD, YYYY') 
    as date, status
    from log;
5. Exit from psql prompt using (ctrl + D )
6. Please run the following file logsAnalysis.py using
	python3 logsAnalysis.py
7. This shows output of all 3 queries on the standard output.

 
