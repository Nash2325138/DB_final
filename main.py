import threading
import time
# from multiprocessing import Queue
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs

accident_id_set = set()


def connect_db(database):
    try:
        conn = psycopg2.connect(database=database, user="db1", password="db001", host="140.114.77.23", port="5432")
    except Exception:
        print("Unable to connect")
    return conn


def prompt_user(conn):
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    while True:
        print("\n1: Show all unclear events")
        print("2: Enter correct location of event")
        print("3: Enter sql command directly")
        print("4: Quit")
        command = input("Command: ")

        if command == "1":
            dict_cur.execute("SELECT * FROM accident_event WHERE accident_status='not clear';")
            result = dict_cur.fetchall()
            for row in result:
                print(row)

        elif command == "2":
            event_id = input("Enter the event id: ")
            try:
                event_id = int(event_id)
            except ValueError:
                print("Please enter a valid integer")
                continue
            dict_cur.execute("SELECT count(*) FROM accident_event WHERE accident_id = %s", (event_id, ))
            if dict_cur.fetchall()[0][0] == 0:
                print("No such event id")
                continue

            latitude = input("Actual latitude: ")
            longitude = input("Actual longitude: ")
            try:
                longitude = float(longitude)
                latitude = float(latitude)
            except ValueError:
                print("Please enter a valid float")
                continue

            try:
                sql = """
                    UPDATE accident_event
                    SET actual_longitude = %s, actual_latitude= %s, accident_status = 'clear'
                    WHERE accident_id = %s;
                """
                dict_cur.execute(sql, (longitude, latitude, event_id))
            except psycopg2.Error as e:
                print(e.diag.message_primary, end='\n\n')
                continue

        elif command == "3":
            sql = input("\nEnter your sql command here: ")
            try:
                dict_cur.execute(sql)
            except psycopg2.Error as e:
                print(e.diag.message_primary, end='\n\n')
                continue
            for row in dict_cur.fetchall():
                print(row)

        elif command == "4":
            return
        else:
            print("There's not command called " + command)


def scan_view(conn1, conn2):
    global accident_id_set
    dict_cur2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)
    dict_cur1 = conn1.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Need initialize set_accident_id: add all accident_id from accident_event of db1
    dict_cur1.execute("SELECT accident_id FROM accident_event")
    for row in dict_cur1.fetchall():
        accident_id_set.add(row['accident_id'])

    # start scanning
    while True:
        time.sleep(1)
        sql = """
            SELECT
            accident_id,
            item_no,
            latitude as actual_longitude,
            longitude as actual_latitude,
            road_id,
            road_direction,
            milage,
            road_type,
            road_section_name
            FROM accident_status_information
            WHERE status_of_the_event = 'not clear'
            -- We can't access table in db1 while in db2
            -- AND accident_id NOT IN (
            --     SELECT accident_id FROM db1.accident_event
            -- );
        """
        dict_cur2.execute(sql)
        rows = dict_cur2.fetchall()
        # print(table)

        for row in rows:
            if row['accident_id'] in accident_id_set:
                continue
            accident_id_set.add(row['accident_id'])

            # new Accident Event
            columns = row.keys()
            columns = [column for column in columns]
            values = [row[column] for column in columns]

            sql = "INSERT INTO accident_event (%s) VALUES %s"
            sql = dict_cur1.mogrify(sql, (AsIs(', '.join(columns)), tuple(values)))
            # print(sql)
            dict_cur1.execute(sql)

            sensor_latitude = row['actual_latitude']
            sensor_longitude = row['actual_longitude']

            # calculate the closest HC
            sql = """
                SELECT health_center_id, health_center_name,
                |/((latitude - %s)^2 + (longitude - %s)^2) AS distance
                FROM health_center
                WHERE |/((latitude - %s)^2 + (longitude - %s)^2) IN (
                    SELECT MIN(|/((latitude - %s)^2 + (longitude - %s)^2))
                    FROM health_center
                );
            """
            dict_cur1.execute(sql, (sensor_latitude, sensor_longitude,
                                    sensor_latitude, sensor_longitude,
                                    sensor_latitude, sensor_longitude))
            closest_HC_name = dict_cur1.fetchall()[0]['health_center_name']

            # calculate the closest PO
            sql = """
                SELECT chinese_name, english_name,
                |/((latitude - %s)^2 + (longitude - %s)^2) AS distance
                FROM police_station
                WHERE |/((latitude - %s)^2 + (longitude - %s)^2) IN (
                    SELECT MIN(|/((latitude - %s)^2 + (longitude - %s)^2))
                    FROM police_station
                );
            """
            dict_cur1.execute(sql, (sensor_latitude, sensor_longitude,
                                    sensor_latitude, sensor_longitude,
                                    sensor_latitude, sensor_longitude))
            closest_PO_name = dict_cur1.fetchall()[0]['english_name']

            # new reponse_unit
            sql = """
                INSERT INTO response_unit (
                    accident_id,
                    item_no,
                    response_police_station,
                    response_health_center,
                    road_id,
                    road_direction,
                    milage
                ) values (%s, %s, %s, %s, %s, %s, %s)
            """
            sql = dict_cur1.mogrify(sql, (
                row['accident_id'],
                row['item_no'],
                closest_PO_name,
                closest_HC_name,
                row['road_id'],
                row['road_direction'],
                row['milage']
            ))
            dict_cur1.execute(sql)
        conn1.commit()
        conn2.commit()


def foobar():
    conn1 = connect_db(database="db1")
    input_thread = threading.Thread(target=prompt_user, args=(conn1, ))
    input_thread.daemon = True

    conn2 = connect_db(database="db2")
    scan_thread = threading.Thread(target=scan_view, args=(conn1, conn2))
    scan_thread.daemon = True

    input_thread.start()
    scan_thread.start()

    input_thread.join()
    conn1.close()
    conn2.close()


foobar()
