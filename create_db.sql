CREATE TABLE IF NOT EXISTS instance_type(
    id serial primary key,
    type varchar(255) not null,
    ondemand_price numeric default 0 CONSTRAINT positive_ondemand_price CHECK (ondemand_price >= 0),
    cpus integer default 0 CONSTRAINT positive_cpu_price CHECK (cpus >= 0),
    memory numeric default 0 CONSTRAINT positive_memory CHECK (memory >= 0),
    disk numeric default 0 CONSTRAINT positive_disk CHECK (disk >= 0),
    ami varchar(255) not null,
    virtualization varchar(255) not null
);

CREATE TABLE IF NOT EXISTS work_instance (
    id serial primary key,
    type int NOT NULL,
    address character varying(255) NOT NULL,
    zone text NOT NULL,
    price numeric NOT NULL,
    ami text NOT NULL,
    state character varying DEFAULT 'Idle'::character varying,
    CONSTRAINT fk1_ins FOREIGN KEY (type) REFERENCES instance_type (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS aws_credentials(
    id serial primary key,
    access_key varchar(255) not null,
    secret_key varchar(255) not null,
    key_pair varchar(255) not null
);

CREATE TABLE IF NOT EXISTS client(
    id serial primary key,
    username varchar UNIQUE NOT NULL,
    aws_credentials_id int NOT NULL,
    CONSTRAINT fk1_cred FOREIGN KEY (aws_credentials_id) REFERENCES aws_credentials (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workload (
    id serial primary key,
    client_id integer,
    executable varchar,
    working_dir varchar,
    CONSTRAINT fk1_client FOREIGN KEY (client_id) REFERENCES client (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS profile_job (
    id serial primary key,
    workload_id int NOT NULL,
    work_instance_id int,
    execution_time int,
    exit_status varchar,
    status varchar,
    results varchar,
    CONSTRAINT fk1_workload FOREIGN KEY (workload_id) REFERENCES workload (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk1_inst FOREIGN KEY (work_instance_id) REFERENCES work_instance (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE
);






