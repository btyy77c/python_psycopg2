#!/usr/bin/env python
#
# news.py -- Reads from a news database and adds
#

import psycopg2

try:
    print "Connecting to Database"
    conn = psycopg2.connect("dbname=news")
    cur = conn.cursor()
except psycopg2.Error as e:
    print "Unable to connect!"
    print e.pgerror
    print e.diag.message_detail
    sys.exit(1)
else:
    print "Connected!"


def close_db_connection():
    """Closes database connection"""
    cur.close()
    conn.close()


def count_articles():
    """Orders articles by view_count and returns top 3"""
    print 'What are the most popular three articles of all time?'
    cur.execute("SELECT \
                   articles.title, \
                   COUNT(log.path) as view_count \
                 FROM articles \
                    INNER JOIN log ON log.path = CONCAT('/article/', articles.slug) \
                 GROUP BY articles.title \
                 ORDER BY view_count DESC \
                 LIMIT 3;")


def count_authors():
    print 'Who are the most popular article authors of all time?'
    cur.execute(" \
      SELECT \
        authors.name, \
        COUNT(log.path) as view_count \
      FROM articles \
        INNER JOIN authors ON articles.author = authors.id \
        INNER JOIN log ON log.path = CONCAT('/article/', articles.slug) \
      GROUP BY authors.id \
      ORDER BY view_count DESC; \
      ")


def count_errors():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    print 'On which days did more than 1% of requests lead to errors?'
    cur.execute(" \
      SELECT \
        time::date as day, \
        round(COUNT(CASE WHEN status <> '200 OK' THEN 1 END)::decimal / \
              COUNT(time::date)::decimal * 100, 1) as error_percent \
      FROM log \
      GROUP BY day \
      HAVING COUNT(CASE WHEN status <> '200 OK' THEN 1 END)::decimal / \
                   COUNT(time::date)::decimal >= 0.01 \
      ORDER BY error_percent DESC, day DESC; \
      ")


def print_rows(end_word):
    """prints the database values in each row"""
    for row in cur:
        print '"{article}" - {count}{end_word}'.format(article=row[0],
                                                       count=row[1],
                                                       end_word=end_word)
    print ''


if __name__ == '__main__':
    count_articles()
    print_rows(' views')

    count_authors()
    print_rows(' views')

    count_errors()
    print_rows('% errors')

    close_db_connection()
