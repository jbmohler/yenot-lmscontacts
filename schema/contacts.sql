create extension if not exists "uuid-ossp";

create schema contacts;

CREATE TABLE contacts.personas (
    id uuid primary key default uuid_generate_v1mc(),
    corporate_entity boolean default false,
    l_name character varying(80), -- not null check(char_length(l_name)>=2),
    f_name character varying(40),
    title character varying(10),
    memo text,
    birthday date,
    anniversary date,
    organization text,
    owner_id uuid references users(id) not null,
    constraint chk_corp_names check(not corporate_entity or (f_name is null and title is null and char_length(l_name) >= 2)),
    constraint chk_indiv_names check(corporate_entity or (char_length(l_name)>=2 or char_length(f_name)>=2))
);

CREATE TABLE contacts.persona_shares (
    persona_id uuid not null references contacts.personas(id),
    user_id uuid not null references users(id),
    primary key (persona_id, user_id)
);

ALTER TABLE contacts.personas
    ADD CONSTRAINT owner_shares_fkey foreign key (id, owner_id)
        references contacts.persona_shares(persona_id, user_id)
        deferrable initially deferred;

CREATE TABLE contacts.tags (
    id uuid primary key default uuid_generate_v1mc(),
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

CREATE SEQUENCE bit_index_seq AS integer START WITH 100;

CREATE TABLE contacts.email_addresses (
    id uuid primary key default uuid_generate_v1mc(),
    persona_id uuid not null references contacts.personas(id),
    bit_sequence integer not null default nextval('bit_index_seq'),
    is_primary boolean not null default false,
    name text,
    memo text,
    email character varying(60)
);

CREATE TABLE contacts.phone_numbers (
    id uuid primary key default uuid_generate_v1mc(),
    persona_id uuid not null references contacts.personas(id),
    bit_sequence integer not null default nextval('bit_index_seq'),
    is_primary boolean not null default false,
    name text,
    memo text,
    number character varying(18)
);

CREATE TABLE contacts.street_addresses (
    id uuid primary key default uuid_generate_v1mc(),
    persona_id uuid not null references contacts.personas(id),
    bit_sequence integer not null default nextval('bit_index_seq'),
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
    bit_sequence integer not null default nextval('bit_index_seq'),
    is_primary boolean not null default false,
    name text,
    memo text,
    url character varying(150),
    username character varying(50),
    password_enc bytea,
    pw_reset_dt date,
    pw_next_reset_dt date
);

create view contacts.perfts_search as
select id,
    to_tsvector(coalesce(l_name, ''))||
    to_tsvector(coalesce(f_name, ''))||
    to_tsvector(coalesce(organization, ''))||
    to_tsvector(coalesce(title, ''))||
    to_tsvector(coalesce(memo, '')) as fts_search
from contacts.personas;

create view contacts.personas_calc as
select personas.*,
    to_tsvector(coalesce(l_name, ''))||
    to_tsvector(coalesce(f_name, ''))||
    to_tsvector(coalesce(organization, ''))||
    to_tsvector(coalesce(title, ''))||
    to_tsvector(coalesce(memo, '')) as fts_search,
    -- see also constraints related to corporate_entity & f_name
    concat_ws(' ',
        case when personas.title='' then null else personas.title end,
        case when personas.f_name='' then null else personas.f_name end,
        case when personas.l_name='' then null else personas.l_name end) as entity_name
from contacts.personas;

create view contacts.bits as
(
    select id, persona_id, 'urls' as bit_type,
        name, memo, is_primary,
        bit_sequence,
        to_tsvector(coalesce(memo, ''))||
        to_tsvector(coalesce(name, ''))||
        to_tsvector(coalesce(url, '')) as fts_search,
        json_build_object(
                'url', url,
                'username', username,
                'password_enc', password_enc,
                'pw_reset_dt', pw_reset_dt,
                'pw_next_reset_dt', pw_next_reset_dt) as bit_data
    from contacts.urls
)union all(
    select id, persona_id, 'street_addresses' as bit_type,
        name, memo, is_primary,
        bit_sequence,
        to_tsvector(coalesce(memo, ''))||
        to_tsvector(coalesce(name, ''))||
        to_tsvector(coalesce(address1, ''))||
        to_tsvector(coalesce(address2, ''))||
        to_tsvector(coalesce(city, '')) as fts_search,
        json_build_object(
                'address1', address1,
                'address2', address2,
                'city', city,
                'state', state,
                'zip', zip,
                'country', country) as bit_data
    from contacts.street_addresses
)union all(
    select id, persona_id, 'phone_numbers' as bit_type,
        name, memo, is_primary,
        bit_sequence,
        to_tsvector(coalesce(memo, ''))||
        to_tsvector(coalesce(name, '')) as fts_search,
        json_build_object(
                'number', number) as bit_data
    from contacts.phone_numbers
)union all(
    select id, persona_id, 'email_addresses' as bit_type,
        name, memo, is_primary,
        bit_sequence,
        to_tsvector(coalesce(memo, ''))||
        to_tsvector(coalesce(name, '')) as fts_search,
        json_build_object(
                'email', email) as bit_data
    from contacts.email_addresses
);

