#! /usr/bin/env python3

import psycopg2

# SQL Query to fetch top 3 articles
TOP3_ARTICLES_VIEWS_SQL = '''select articles.title, count(log.path) as views
from log
join articles
on log.path = concat('/article/', articles.slug)
group by articles.title
order by views desc
limit 3;
'''

# SQL Query to fetch authors sorted by sum of their article views
POPULAR_ARTICLES_AUTHOR_SQL = '''select title_num_name.name, sum(num)
as num_of_view from
(select articles.title, count(log.path) as num, authors.name
from log, articles,authors
where log.path like '%' || articles.slug
and authors.id = articles.author group by articles.title, authors.name
order by authors.name) as title_num_name
group by title_num_name.name
order by num_of_view desc;
'''

# SQL Query to find the days where number of bad requests are > 1%
# Used with clause because I could not filter percentage
# by using percentage column alias.

DATES_BAD_REQUESTS_SQL = '''with date_status_ok_bad_percentage as (
    select requests_bad.date as date,
           requests_bad.bad_requests_count,
           requests_total.total_requests_count,
           (requests_bad.bad_requests_count * 100::numeric
           / requests_total.total_requests_count) as percentage
    from
        (select date, count(status) as bad_requests_count
        from date_status_code
        where status = '404 NOT FOUND'
        group by date
        order by date)
             as requests_bad
        join
        (select date, count(status) as total_requests_count
        from date_status_code
        group by date
        order by date)
            as requests_total
        on requests_bad.date = requests_total.date
)
select date, round(percentage, 2) from date_status_ok_bad_percentage
where percentage > 1;
'''


def connect_db(database_name="news"):
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor

    except ConnectionError as e:
        print("Error connecting to database", e)


def get_sql_output(cursor, sql_query):
    cursor.execute(sql_query)
    return cursor.fetchall()


def execute_sql_queries():
    db, my_cursor = connect_db()
    top3_popular_articles = get_sql_output(my_cursor, TOP3_ARTICLES_VIEWS_SQL)
    print("*" * 85)
    print("Top 3 articles sorted by views are : ")
    for article in top3_popular_articles:
        print(" Article :- {} \t No. of Views:- {}".
              format(article[0], article[1]))

    print("*" * 85)
    popular_authors = get_sql_output(my_cursor, POPULAR_ARTICLES_AUTHOR_SQL)
    print("Popular authors by summing all their article views: ")
    for author in popular_authors:
        print(" Author:- {} \t No. of Views:- {}".
              format(author[0], author[1]))

    print("*" * 85)
    bad_request_days = get_sql_output(my_cursor, DATES_BAD_REQUESTS_SQL)
    print("Printing days when there are more than 1% of bad requests: ")
    for day in bad_request_days:
        print(" Day :- {} \t errors:- {}%".
              format(day[0], day[1]))
    print("*" * 85)

    db.close()


if __name__ == '__main__':
    execute_sql_queries()
