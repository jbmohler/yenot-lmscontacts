create extension if not exists "uuid-ossp";

create schema contacts;

CREATE TABLE contacts.personas (
    id uuid primary key default uuid_generate_v1mc(),
    l_name character varying(80) not null check(char_length(l_name)>=2),
    f_name character varying(40),
    title character varying(10),
    memo text,
    birthday date,
    anniversary date,
    organization text
);

CREATE TABLE contacts.tags (
    id uuid primary key,
    name character varying(40) not null check(char_length(name)>=2),
    parent_id uuid
);

CREATE TABLE contacts.personaassoc (
    umbrella_persona_id uuid NOT NULL references contacts.personas(id),
    individual_persona_id uuid NOT NULL references contacts.personas(id)
);

CREATE TABLE contacts.tagpersona (
    tag_id uuid not null references contacts.tags(id),
    persona_id uuid not null references contacts.personas(id)
);

CREATE TABLE contacts.email_addresses (
    id uuid primary key default uuid_generate_v1mc(),
    persona_id uuid not null references contacts.personas(id),
    is_primary boolean not null default false,
    name text,
    memo text,
    email character varying(60)
);

CREATE TABLE contacts.phone_numbers (
    id uuid primary key default uuid_generate_v1mc(),
    persona_id uuid not null references contacts.personas(id),
    is_primary boolean not null default false,
    name text,
    memo text,
    number character varying(18)
);

CREATE TABLE contacts.street_addresses (
    id uuid primary key default uuid_generate_v1mc(),
    persona_id uuid not null references contacts.personas(id),
    is_primary boolean not null default false,
    name text,
    memo text,
    address1 text,
    address2 text,
    city text,
    state text,
    zip text,
    country text
);

CREATE TABLE contacts.urls (
    id uuid primary key default uuid_generate_v1mc(),
    persona_id uuid not null references contacts.personas(id),
    is_primary boolean not null default false,
    name text,
    memo text,
    url character varying(150),
    username character varying(50),
    password character varying(50)
);
