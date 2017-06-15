import sys
import threading
import time
# from multiprocessing import Queue
import psycopg2

set_item_no = set()


def connect_db(database):
    try:
        conn = psycopg2.connect(database=database, user="db1", password="db001", host="140.114.77.23", port="5432")
    except Exception:
        print("Unable to connect")
    return conn


def prompt_user(conn):
    cursor = conn.cursor()
    while True:
        print("\n1: Show all unclear events")
        print("2: Enter correct location of event")
        print("3: Enter sql command directly")
        print("4: Quit")
        command = input("Command: ")

        if command == "1":
            cursor.execute("SELECT * FROM accident_event WHERE accident_status='not clear';")
            for row in cursor.fetchall():
                print(row)

        elif command == "2":
            event_id = input("Enter the event id: ")
            try:
                event_id = int(event_id)
            except ValueError:
                print("You must enter a integer")
                continue
            cursor.execute("SELECT count(*) FROM accident_event WHERE accident_id = %s", (event_id, ))
            if cursor.fetchall()[0][0] == 0:
                print("No such event id")
                continue

            longitude = input("Actual longitude: ")
            latitude = input("Actual latitude: ")
            try:
                longitude = float(longitude)
                latitude = float(latitude)
            except ValueError:
                print("You must enter a float")
                continue

            try:
                sql = """UPDATE accident_event
                SET actual_longitude = %s, actual_latitude= %s
                WHERE accident_id = %s;"""
                cursor.execute(sql, (longitude, latitude, event_id))
            except psycopg2.Error as e:
                print(e.diag.message_primary, end='\n\n')
                continue

        elif command == "3":
            sql = input("\nEnter your sql command here: ")
            try:
                cursor.execute(sql)
            except psycopg2.Error as e:
                print(e.diag.message_primary, end='\n\n')
                continue
            for row in cursor.fetchall():
                print(row)

        else:
            print("There's not command called " + command)


def scan_view(conn):
    global set_item_no
    cursor = conn.cursor()
    while True:
        time.sleep(1)
        sql = "SELECT * FROM accident_status_information;"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            pass
            item_no = row[1]
            if item_no not in set_item_no:
                print("The accident", item_no, "is not in original set_item_no")
                # insert into Accident Event
                # calculate the closest HC/PO

                set_item_no.add(item_no)
                pass


def foobar():
    conn1 = connect_db(database="db1")
    input_thread = threading.Thread(target=prompt_user, args=(conn1, ))
    input_thread.daemon = True
    input_thread.start()

    conn2 = connect_db(database="db2")
    scan_thread = threading.Thread(target=scan_view, args=(conn2, ))
    scan_thread.daemon = True
    scan_thread.start()

    input_thread.join()
    conn1.close()
    conn2.close()


foobar()
