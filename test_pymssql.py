import pytest
import pymssql

@pytest.fixture()
def db_connection():
    connection = pymssql.connect(
        server='localhost\\sql2022d',
        user='developer',
        password='developer',
        database='StackOverflow2013'
    )
    return connection


def get_posts():
    posts = [
        (
            24626249, # PostId
            'be08c304-b35d-48a0-9e14-b7b3c295e513', # RandomId
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
            '2012-10-31 16:42:47.213', # CommunityOwnedDate
            '2008-07-31 21:42:52.667', # CreationDate
            41, # FavoriteCount
            '2018-07-02 17:55:27.247', # LasActivityDate
            '2018-07-02 17:55:27.247', # LastEditDate
            'Rich B', # LastEditorDisplayName
            6786713, # LastEditorUserId
            8, # OwnerUserId
            0, # ParentId
            1, # PostTypeId
            573.911, # Score
            '<c#><floating-point><type-conversion><double><decimal>', # Tags
            'Convert Decimal to Double?', # Title
            37080, # ViewCount
            '2024-11-16', # InsertedDate
            '00:25:59', # InsertedHour
            '0xaa02f18d37d95592515b4c06c2f0a22b4762100aabb68759976e3851f55c7ef9'.encode('utf-8') # PostHash
        )
    ]
    return posts

def test_pymssql_bulk_insert_all_columns_to_sql_server(db_connection):
    cursor=db_connection.cursor(as_dict=True) # as_dict=True returns rows as Python dictionaries
    cursor.execute("truncate table dbo.Posts_His")
    db_connection.commit()

    col_data = get_posts()
    
    db_connection.bulk_copy(table_name='posts_his', elements=col_data, column_ids=None, batch_size=2)
    db_connection.commit()

    cursor=db_connection.cursor(as_dict=True)
    cursor.execute("SELECT count(1) as registers FROM Posts_His")
    row= cursor.fetchone()
    while row:
        assert row['registers'] == 1
        row=cursor.fetchone()


def test_pymssql_bulk_insert_some_columns_to_sql_server(db_connection):
    cursor=db_connection.cursor(as_dict=True)
    cursor.execute("truncate table dbo.Posts_His")
    db_connection.commit()

    col_data=[
        (24626249,12.5,'2008-07-31 21:42:52.667'),
        (24626251,120.9,'2012-08-01 19:15:11.185')
        ]

    db_connection.bulk_copy(
        table_name='posts_his', 
        elements=col_data, 
        column_ids=[1,3,10], 
        batch_size=2
        )
    db_connection.commit()

    cursor=db_connection.cursor(as_dict=True)
    cursor.execute("SELECT count(1) as registers FROM Posts_His")
    row= cursor.fetchone()
    while row:
        assert row['registers'] == 2
        row=cursor.fetchone()


def test_pymssql_insert_nvarchar_columns_to_sql_server(db_connection):
    cursor=db_connection.cursor(as_dict=True)
    cursor.execute("truncate table dbo.Posts_His")
    db_connection.commit()

    cursor.execute(
        """insert into dbo.Posts_His(PostId, RandomId, OriginalId, AcceptedAnswerId, AnswerCount, Body, ClosedDate, CommentCount,
                 CommunityOwnedDate, CreationDate, FavoriteCount, LastActivityDate, LastEditDate, LastEditorDisplayName,
                 LastEditorUserId, OwnerUserId, ParentId, PostTypeId, Score, Tags, Title, ViewCount, InsertedDate,
                 InsertedHour,PostHash)
                 VALUES(
                 %d,
                 %s,
                 %d,
                 %d,
                 %d,
                 %s,
                 %s,
                 %d,
                 %s,
                 %s,
                 %d,
                 %s,
                 %s,
                 %s,
                 %d,
                 %d,
                 %d,
                 %d,
                 %d,
                 %s,
                 %s,
                 %d,
                 %s,
                 %s,
                 %s
                 )""",
                  (
                24626249, # PostId
                'be08c304-b35d-48a0-9e14-b7b3c295e513', # RandomId
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
                '2012-10-31 16:42:47.213', # CommunityOwnedDate
                '2008-07-31 21:42:52.667', # CreationDate
                41, # FavoriteCount
                '2018-07-02 17:55:27.247', # LasActivityDate
                '2018-07-02 17:55:27.247', # LastEditDate
                'Rich B', # LastEditorDisplayName
                6786713, # LastEditorUserId
                8, # OwnerUserId
                0, # ParentId
                1, # PostTypeId
                573.911, # Score
                '<c#><floating-point><type-conversion><double><decimal>', # Tags
                'Convert Decimal to Double?', # Title
                37080, # ViewCount
                '2024-11-16', # InsertedDate
                '00:25:59', # InsertedHour
                bytearray('0xaa02f18d37d95592515b4c06c2f0a22b4762100aabb68759976e3851f55c7ef9'.encode('utf-8')) # PostHash
                # '0xaa02f18d37d95592515b4c06c2f0a22b4762100aabb68759976e3851f55c7ef9'.encode('utf-8') # this does not work for `INSERT INTO`
            )
        )
    db_connection.commit()

    cursor=db_connection.cursor(as_dict=True)
    cursor.execute("SELECT count(1) as registers FROM Posts_His")
    row= cursor.fetchone()
    while row:
        assert row['registers'] == 1
        row=cursor.fetchone()