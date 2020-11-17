create table NEWS
(
    id          bigint primary key auto_increment,
    title       text,
    content     text,
    url         varchar(1000),
    created_at  datetime default now(),
    modified_at datetime default now()
) default charset = utf8mb4;
create table LINKS_TO_BE_PROCESSED
(
    links varchar(1000)
);
create table LINKS_ALREADY_PROCESSED
(
    links varchar(1000)
)