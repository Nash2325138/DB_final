import sys
import threading
import time
# from multiprocessing import Queue
import psycopg2


def connect_db():
    try:
        conn = psycopg2.connect(database="db1", user="db1", password="db001", host="140.114.77.23", port="5432")
    except Exception:
        print("Unable to connect")
    return conn


def prompt_user(cursor):
    while True:
        print("1: Enter sql command directly")
        print("2: Show all unclear events")
        print("3: Enter correct location of event")
        command = sys.stdin.readline()[:-1]
        print()
        if command == "1":
            print("Enter your sql command: ", end='')
            sql = sys.stdin.readline()
            cursor.execute(sql)
            rows = cursor.fetchall()
            print(rows)
        elif command == "2":
            pass
        elif command == "3":
            pass
        else:
            print("There's not command called " + command)


def scan_view(cursor):
    while True:
        time.sleep(1)
        print("scanning...")
        # scan the view


def foobar(cursor):
    input_thread = threading.Thread(target=prompt_user, args=(cursor, ))
    input_thread.daemon = True
    input_thread.start()

    scan_thread = threading.Thread(target=scan_view, args=(cursor, ))
    scan_thread.daemon = True
    scan_thread.start()

    input_thread.join()


conn = connect_db()
cursor = conn.cursor()
foobar(cursor)

cursor.close()
conn.close()
