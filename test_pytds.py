import pytest

import pytds
from pytds.tds_base import Column

from datetime import datetime
import uuid

@pytest.fixture()
def db_connection():
    conn = pytds.connect(dsn='localhost\\sql2022d', 
                         database='StackOverflow2013', 
                         user='developer', 
                         password='developer')
    return conn

         
def get_posts():
    date_string = "2008-07-31 21:42:52.667"
    date_format = "%Y-%m-%d %H:%M:%S.%f"
    posts = [
        [        
            24626249, # PostId
            uuid.UUID('be08c304-b35d-48a0-9e14-b7b3c295e513'), # RandomId
            4, # OriginalId
            7, # AcceptedAnswerId
            13, # AnswerCount
            """<p>I want to use a track-bar to change a form''s opacity.</p>

            <p>This is my code:</p>

            <pre><code>decimal trans = trackBar1.Value / 5000;
            this.Opacity = trans;
            </code></pre>

            <p>When I build the application, it gives the following error:</p>

            <blockquote>
            <p>Cannot implicitly convert type <code>'' decimal ''</code> to <code>''double''</code>.</p>
                    </blockquote>

            <p>I tried using <code>trans</code> and <code>double</code> but then the control doesn''t work. This code worked fine in a past VB.NET project.</p>

            """, # Body
            None, # ClosedDate
            1, # CommentCount
            datetime.strptime('2012-10-31 16:42:47.213',date_format), # CommunityOwnedDate
            datetime.strptime(date_string,date_format), # CreationDate
            41, # FavoriteCount
            datetime.strptime('2018-07-02 17:55:27.247',date_format), # LastActivityDate
            datetime.strptime('2018-07-02 17:55:27.247',date_format), # LasEditDate
            'Rich B', # LasEditorDisplayName
            6786713, # LasEditorUserId
            8, # OwnerUserId
            0, # ParentId
            1, # PostTypeId
            573.911, # Score
            '<c#><floating-point><type-conversion><double><decimal>', # Tags
            'Convert Decimal to Double?', # Title
            37080, # ViewCount
            datetime.strptime('2024-11-16','%Y-%m-%d'), # InsertedDate
            datetime.strptime('00:25:59','%H:%M:%S').time(), # InsertedHour
            '0xaa02f18d37d95592515b4c06c2f0a22b4762100aabb68759976e3851f55c7ef9'.encode('utf-8') # PostHash
        ]
    ]
    return posts

def test_pytds_bulk_insert_all_columns_sql_server(db_connection):
    with db_connection.cursor() as cursor:

        cursor.execute("truncate table dbo.Posts_His")
        db_connection.commit()

        column_types = [
            Column("PostId", type=pytds.tds_types.BigIntType(),flags=0), # set flags=0 as primary keys are NOT NULL
            Column("RandomId", type=pytds.tds_types.UniqueIdentifierType()),
            Column("OriginalId", type=pytds.tds_types.IntType()),
            Column("AcceptedAnswerId", type=pytds.tds_types.IntType()),
            Column("AnswerCount", type=pytds.tds_types.IntType()),
            Column("Body", type=pytds.tds_types.NVarCharMaxType()),
            Column("ClosedDate", type=pytds.tds_types.DateTime2Type()),
            Column("CommentCount", type=pytds.tds_types.IntType()),
            Column("CommunityOwnedDate", type=pytds.tds_types.DateTime2Type()),
            Column("CreationDate", type=pytds.tds_types.DateTimeType(),flags=0), # set flags=0 for NOT NULL columns
            Column("FavoriteCount", type=pytds.tds_types.IntType()),
            Column("LastActivityDate", type=pytds.tds_types.DateTime2Type()),
            Column("LastEditDate", type=pytds.tds_types.DateTime2Type()),
            Column("LastEditorDisplayName", type=pytds.tds_types.NVarCharType(40)),
            Column("LastEditorUserId", type=pytds.tds_types.IntType()),
            Column("OwnerUserId", type=pytds.tds_types.IntType()),
            Column("ParentId", type=pytds.tds_types.IntType()),
            Column("PostTypeId", type=pytds.tds_types.IntType()),
            Column("Score", type=pytds.tds_types.DecimalType(10,4)),
            Column("Tags", type=pytds.tds_types.VarCharType(150)),
            Column("Title", type=pytds.tds_types.NVarCharType(250)),
            Column("ViewCount", type=pytds.tds_types.IntType()),
            Column("InsertedDate", type=pytds.tds_types.DateType()),
            Column("InsertedHour", type=pytds.tds_types.TimeType()),
            Column("PostHash", type=pytds.tds_types.VarBinaryType(1000))
        ]


        posts=get_posts()

        cursor.copy_to(data=posts,
                        table_or_view= 'Posts_His',
                        schema='dbo', 
                        columns=column_types,
                        rows_per_batch=2
                        ) 

        db_connection.commit()

        cursor.execute("select count(1) from dbo.Posts_His")
        assert cursor.fetchone()[0]==1


def test_pytds_bulk_insert_some_columns_sql_server(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute("truncate table dbo.Posts_His")
        db_connection.commit()

        column_types = [
            Column("PostId", type=pytds.tds_types.BigIntType(),flags=0),
            Column("CreationDate", type=pytds.tds_types.DateTimeType(),flags=0), # set flags=0 for NOT NULL columns
            Column("Tags", type=pytds.tds_types.VarCharType(150)),
        ]

        posts=get_posts()

        # Get the first, ninth, and nineteenth columns
        posts=[post[0:1]+post[9:10]+post[19:20] for post in posts]

        cursor.copy_to(data=posts,
                        table_or_view= 'Posts_His',
                        schema='dbo', 
                        columns=column_types,
                        rows_per_batch=2) 

        db_connection.commit()

        cursor.execute("select count(1) from dbo.Posts_His")
        assert cursor.fetchone()[0]==1


def read_posts_from_file():
    with open('Posts_His.tsv', 'r',encoding='utf8') as file:
        for line in file:
            yield line

def test_pytds_bulk_insert_from_file_to_sql_server(db_connection):
   with db_connection.cursor() as cursor:
        cursor.execute("truncate table dbo.Posts_His")
        db_connection.commit()

        column_types = [
            Column("PostId", type=pytds.tds_types.NVarCharType(4000),flags=0),
            Column("RandomId", type=pytds.tds_types.NVarCharType(4000)),
            Column("OriginalId", type=pytds.tds_types.NVarCharType(4000)),
            Column("AcceptedAnswerId", type=pytds.tds_types.NVarCharType(4000)),
            Column("AnswerCount", type=pytds.tds_types.NVarCharType(4000)),
            Column("Body", type=pytds.tds_types.NVarCharMaxType()),
            Column("ClosedDate", type=pytds.tds_types.NVarCharType(4000)),
            Column("CommentCount", type=pytds.tds_types.NVarCharType(4000)),
            Column("CommunityOwnedDate", type=pytds.tds_types.NVarCharType(4000)),
            Column("CreationDate", type=pytds.tds_types.NVarCharType(4000),flags=0), # set flags=0 for NOT NULL columns
            Column("FavoriteCount", type=pytds.tds_types.NVarCharType(4000)),
            Column("LastActivityDate", type=pytds.tds_types.NVarCharType(4000)),
            Column("LastEditDate", type=pytds.tds_types.NVarCharType(4000)),
            Column("LastEditorDisplayName", type=pytds.tds_types.NVarCharType(4000)),
            Column("LastEditorUserId", type=pytds.tds_types.NVarCharType(4000)),
            Column("OwnerUserId", type=pytds.tds_types.NVarCharType(4000)),
            Column("ParentId", type=pytds.tds_types.NVarCharType(4000)),
            Column("PostTypeId", type=pytds.tds_types.NVarCharType(4000)),
            Column("Score", type=pytds.tds_types.NVarCharType(4000)),
            Column("Tags", type=pytds.tds_types.NVarCharType(4000)),
            Column("Title", type=pytds.tds_types.NVarCharType(4000)),
            Column("ViewCount", type=pytds.tds_types.NVarCharType(4000)),
            Column("InsertedDate", type=pytds.tds_types.NVarCharType(4000)),
            Column("InsertedHour", type=pytds.tds_types.NVarCharType(4000)),
            #Column("PostHash", type=pytds.tds_types.NVarCharType(4000)) # VARBINARY does not work for files
        ]


        cursor.copy_to(
                    file= read_posts_from_file(),
                    table_or_view= 'Posts_His',
                    columns=column_types,
                    schema='dbo',
                    sep='\t'
                    ) 

        db_connection.commit()

        cursor.execute("select count(1) from dbo.Posts_His")
        assert cursor.fetchall()==[(2,)]