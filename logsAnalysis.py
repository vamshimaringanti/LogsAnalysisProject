import psycopg2

DBNAME = "news"

# SQL Query to fetch top 3 articles
TOP3_ARTICLES_VIEWS_SQL = '''select articles.title, count(log.path) as num
from log, articles
where log.path like '%' || articles.slug
group by articles.title
order by num desc
limit 3;'''

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
# date_status_code view has formatted date & status entries like this
# July 15, 2016 | 200 OK
# Had to use with clause because I could not filter percentage
# by using column alias.

DATES_BAD_REQUESTS_SQL = '''create or replace view date_status_code as
    select to_char(time, 'FMMonth DD, YYYY') as date, status
    from log;

with date_status_ok_bad_percentage as (
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

try:
    db = psycopg2.connect(database=DBNAME)
    my_cursor = db.cursor()
    my_cursor.execute(TOP3_ARTICLES_VIEWS_SQL)
    top3_popular_aritcles = my_cursor.fetchall()
    print("*" * 85)
    print("Top 3 articles sorted by views are : ")
    for article in top3_popular_aritcles:
        print(" Article :- {} \t No. of Views:- {}".
              format(article[0], article[1]))
    print("*" * 85)
    my_cursor.execute(POPULAR_ARTICLES_AUTHOR_SQL)
    popular_authors = my_cursor.fetchall()
    print("Popular authors by summing all their article views: ")
    for author in popular_authors:
        print(" Author:- {} \t No. of Views:- {0: >8}".
              format(author[0], author[1]))
    print("*" * 85)
    my_cursor.execute(DATES_BAD_REQUESTS_SQL)
    bad_request_days = my_cursor.fetchall()
    print("Printing days when there are more than 1% of bad requests: ")
    for day in bad_request_days:
        print(" Day :- {} \t errors:- {}%".
              format(day[0], day[1]))
    print("*" * 85)
    db.close()

except ConnectionError as e:
    print(e)
