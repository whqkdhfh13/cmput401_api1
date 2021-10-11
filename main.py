from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from sqlite3 import Error, connect, version


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = connect(db_file, check_same_thread=False)
    except Error as e:
        print(e)

    return conn


def create_table_books():
    sql_script_make_books = """
                                CREATE TABLE IF NOT EXISTS books (
                                    title text NOT NULL,
                                    isbn13 text PRIMARY KEY,
                                    details text,
                                    publisher text,
                                    year integer,
                                    price real
                                );
                            """
    try:
        c = conn.cursor()
        c.execute(sql_script_make_books)
        c.close()
    except Error as e:
        print(e)


def create_book(book):
    """
    Create a new book
    :param conn:
    :param book:
    """
    script = ''' INSERT INTO books(title, isbn13, details, publisher, year, price) VALUES(?,?,?,?,?,?)'''
    c = conn.cursor()
    c.execute(script, book)
    conn.commit()
    c.close()


def update_book(update_list, isbn13):
    """
    update book
    :param conn:
    :param book:
    :param isbn13:
    """
    c = conn.cursor()
    for key, value in update_list.items():
        c.execute('UPDATE books SET %s = ? WHERE isbn13 = ?' %
                  key, [value] + [isbn13])
    conn.commit()
    c.close()


def delete_book(isbn13):
    """
    Delete a book by isbn13
    :param conn:  Connection to the SQLite database
    :param isbn13: isbn13
    :return:
    """
    c = conn.cursor()
    c.execute('DELETE FROM books WHERE isbn13=?', (isbn13,))
    conn.commit()
    c.close()


def select_books():
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    c = conn.cursor()
    c.execute("SELECT * FROM books")

    rows = c.fetchall()
    c.close()

    return rows


def select_book_by_isbn13(isbn13):
    """
    Query a book by isbn13
    :param conn: the Connection object
    :param isbn13:
    :return:
    """
    c = conn.cursor()
    c.execute("SELECT * FROM books WHERE isbn13=?", (isbn13,))

    row = c.fetchone()

    return row


app = FastAPI()
conn = create_connection('books.db')
api_key = 'hK0iP5dL7bW3fP3y'

if __name__ == '__main__':
    # test
    c = conn.cursor()
    c.execute("DELETE FROM books")
    create_table_books()

    test_isbn13 = 12321321
    test_book = ("hey", test_isbn13, "details1", "publisher2", 2021, 15.00)

    create_book(test_book)
    print(select_book_by_isbn13(test_isbn13))

    update_book(('newbook', "newdetail", "newpub", 1999, 9999.99), test_isbn13)
    print(select_book_by_isbn13(test_isbn13))

    delete_book(test_isbn13)
    print(select_book_by_isbn13(test_isbn13))


async def api_key_check(request):
    try:
        if request.query_params["apikey"] != api_key:
            raise HTTPException(401)
    except:
        raise HTTPException(401)


@app.get("/books")
async def select_books_app(request: Request):
    await api_key_check(request)
    books = select_books()
    temp = {"total": len(books), "books": books}
    print(temp)

    return temp


@app.get("/books/{isbn13}")
async def select_book_app(request: Request, isbn13: str):
    await api_key_check(request)

    book = select_book_by_isbn13(isbn13)
    print(book)
    return book


@app.post("/books")
async def create_book_app(request: Request):
    await api_key_check(request)
    data = await request.json()
    try:
        create_book(
            (data["title"],
             data["isbn13"],
             data["details"],
             data["publisher"],
             int(data["year"]),
             float(data["price"]),)
        )
    except:
        return {
            "status": 1,
            "message": "Book already exists, use PUT to update",
        }

    return {
        "status": 0,
        "message": "Book added",
    }


@app.put("/books/{isbn13}")
async def update_book_app(request: Request, isbn13: str):
    await api_key_check(request)

    data = await request.json()
    print(f"{data=}")
    try:
        update_book(data, isbn13)
    except:
        return {
            "status": 1,
            "message": "Error occured",
        }

    return {
        "status": 0,
        "message": "Book updated",
    }


@app.delete("/books/{isbn13}")
async def delete_book_app(request: Request, isbn13: str):
    await api_key_check(request)

    try:
        delete_book(isbn13)
    except:
        return {
            "status": 1,
            "message": "Error occured",
        }

    return {
        "status": 0,
        "message": "Book deleted",
    }
