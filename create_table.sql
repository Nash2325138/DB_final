create table health_center(
    health_center_id varchar(10),
    health_center_name varchar(50),
    region varchar(10),
    zip_code integer,
    address varchar(60),
    latitude double precision,
    longitude double precision,
    phone_number varchar(30),
    url varchar(100)
);
copy health_center from 'file_path' delimiter ',' csv;

create table police_station(chinese_name varchar(20),
    english_name varchar(60),
    zip_code integer,
    address varchar(60),
    phone_number varchar(20),
    latitude double precision,
    longitude double precision
);
\copy police_station from 'file_path' delimiter E'\t' csv;

create table response_unit(
    response_id serial primary key,
    accident_id integer,
    item_no integer,
    response_police_station varchar(60),
    response_health_center varchar(60),
    road_id varchar(10),
    road_direction varchar(10),
    milage float
);

create table accident_event(
    accident_id integer primary key,
    accident_status varchar(20) DEFAULT 'not clear',
    item_no integer,
    road_id varchar(10),
    road_type integer,
    road_section_name varchar(100),
    road_direction varchar(10),
    milage float,
    actual_longitude double precision,
    actual_latitude double precision
);

create view accident_process_response as (
    select accident_id, accident_status
    from accident_event
);