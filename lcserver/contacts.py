import os
import uuid
import json
import cryptography.fernet
from bottle import request
import rtlib
import yenot.backend.api as api

app = api.get_global_app()


def fernet_keyed():

    key = os.environ["LMS_CONTACTS_KEY"].encode("utf8")

    return cryptography.fernet.Fernet(key)


def get_api_personas_list_prompts():
    return api.PromptList(
        frag=api.cgen.basic(label="Search"),
        tag_id=api.cgen.contact_tag.id(label="Tag"),
        __order__=["frag", "tag_id"],
    )


@app.get(
    "/api/personas/list",
    name="get_api_personas_list",
    report_prompts=get_api_personas_list_prompts,
    report_title="Contact List",
)
def get_api_personas_list():
    frag = request.query.get("frag", None)
    included = request.query.get("included", None)
    tag = request.query.get("tag_id", None)

    select = """
select personas.id, 
    concat_ws(' ',
        case when personas.title='' then null else personas.title end,
        case when personas.f_name='' then null else personas.f_name end,
        case when personas.l_name='' then null else personas.l_name end) as entity_name,
    personas.l_name, personas.f_name, personas.title, personas.organization
from contacts.personas
join contacts.perfts_search fts on personas.id=fts.id
where /*WHERE*/
"""

    params = {}
    wheres = []
    if frag not in ["", None] and included not in ["", None]:
        params["frag"] = api.sanitize_fts(frag)
        params["idlist"] = tuple(included.split(";"))
        wheres.append(
            "(fts.fts_search @@ to_tsquery(%(frag)s) or fts.id in %(idlist)s)"
        )
    elif frag in ["", None] and included not in ["", None]:
        params["idlist"] = tuple(included.split(";"))
        wheres.append("fts.id in %(idlist)s")
    elif frag not in ["", None] and included in ["", None]:
        params["frag"] = api.sanitize_fts(frag)
        wheres.append("fts.fts_search @@ to_tsquery(%(frag)s)")
    if tag not in ["", None]:
        params["tag"] = tag
        wheres.append(
            "(select count(*) from contacts.tagpersona where tag_id=%(tag)s and persona_id=personas.id)>0"
        )

    if len(wheres) == 0:
        wheres.append("True")
    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            id=api.cgen.lms_personas_persona.surrogate(),
            entity_name=api.cgen.lms_personas_persona.name(
                url_key="id", represents=True
            ),
            l_name=api.cgen.auto(hidden=True),
            f_name=api.cgen.auto(hidden=True),
            title=api.cgen.auto(hidden=True),
        )
        results.tables["personas", True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()


def _get_api_persona(a_id=None, newrow=False):
    select = """
select
    concat_ws(' ',
        case when personas.title='' then null else personas.title end,
        case when personas.f_name='' then null else personas.f_name end,
        case when personas.l_name='' then null else personas.l_name end) as entity_name,
    personas.*,
    taglist.tag_ids
from contacts.personas
join lateral (
    select array_agg(tagpersona.tag_id::text) as tag_ids
    from contacts.tagpersona
    where tagpersona.persona_id=personas.id) taglist on true
where /*WHERE*/"""

    select_bits = """
select id, persona_id, bit_type, 
    name, memo, is_primary,
    bit_data
from contacts.bits
where /*BWHERE*/"""

    wheres = []
    bwheres = []
    params = {}
    if a_id != None:
        params["i"] = a_id
        wheres.append("personas.id=%(i)s")
        bwheres.append("bits.persona_id=%(i)s")
    if newrow:
        wheres.append("False")
        bwheres.append("False")

    assert len(wheres) == 1
    select = select.replace("/*WHERE*/", wheres[0])
    select_bits = select_bits.replace("/*BWHERE*/", bwheres[0])

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select, params)

        if newrow:

            def default_row(index, row):
                row.id = str(uuid.uuid1())

            rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables["persona", True] = columns, rows
        bit_colrows = api.sql_tab2(conn, select_bits, params)

        # use the key to decrypt the password replacing column password_enc
        # with password
        f = fernet_keyed()

        def decrypt(oldrow, row):
            if (
                "password_enc" in oldrow.bit_data
                and oldrow.bit_data["password_enc"] != None
            ):
                # the dictionary from postgres comes through with password_enc as a string
                penc = oldrow.bit_data["password_enc"]
                if penc[:2] != r"\x":
                    raise ValueError("expecting hex data prefixed by \\x")
                penc = bytes.fromhex(penc[2:])
                row.bit_data["password"] = f.decrypt(penc).decode("utf8")
                del row.bit_data["password_enc"]

        rows = api.tab2_rows_transform(bit_colrows, bit_colrows[0], decrypt)

        results.tables["bits"] = bit_colrows[0], rows
    return results


@app.get("/api/persona/<a_id>", name="get_api_persona")
def get_api_persona(a_id):
    results = _get_api_persona(a_id)
    return results.json_out()


@app.get("/api/persona/new", name="get_api_persona_new")
def get_api_persona_new():
    results = _get_api_persona(newrow=True)
    results.keys["new_row"] = True
    return results.json_out()


@app.put("/api/persona/<per_id>", name="put_api_persona")
def put_api_persona(per_id):
    persona = api.table_from_tab2(
        "persona",
        amendments=["id"],
        options=[
            "corporate_entity",
            "l_name",
            "f_name",
            "title",
            "organization",
            "memo",
            "anniversary",
            "birthday",
        ],
    )
    try:
        tagdeltas = api.table_from_tab2(
            "tagdeltas", required=["tags_add", "tags_remove"]
        )
    except KeyError:
        tagdeltas = None

    if len(persona.rows) != 1 or persona.rows[0].id != per_id:
        raise api.UserError(
            "invalid-input", "There must be exactly one row and it must match the url."
        )
    if tagdeltas != None and len(tagdeltas.rows) != 1:
        raise api.UserError("invalid-input", "There must be exactly one tagdeltas row.")

    for row in persona.rows:
        if row.corporate_entity:
            if row.f_name is not None:
                if row.f_name == "":
                    row.f_name = None
                else:
                    raise api.UserError(
                        "invalid-input",
                        "Corporate entities must have blank title and f_name.",
                    )
            if row.title is not None:
                if row.title == "":
                    row.title = None
                else:
                    raise api.UserError(
                        "invalid-input",
                        "Corporate entities must have blank title and title.",
                    )

    insert_adds = """
insert into contacts.tagpersona (tag_id, persona_id)
select unnest(%(adds)s)::uuid, %(per)s"""
    # TODO:  need unique constraint
    # on conflict (tag_id, persona_id) do nothing"""

    delete_removes = """
delete from contacts.tagpersona
where tag_id in %(removes)s and persona_id=%(per)s"""

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows("contacts.personas", persona)
        if tagdeltas:
            if tagdeltas.rows[0].tags_add:
                api.sql_void(
                    conn,
                    insert_adds,
                    {"adds": tagdeltas.rows[0].tags_add, "per": per_id},
                )
            if tagdeltas.rows[0].tags_remove:
                api.sql_void(
                    conn,
                    delete_removes,
                    {"removes": tuple(tagdeltas.rows[0].tags_remove), "per": per_id},
                )

        payload = json.dumps({"id": per_id})
        api.sql_void(conn, "notify personas, %(payload)s", {"payload": payload})
        conn.commit()

    return api.Results().json_out()


@app.delete("/api/persona/<per_id>", name="delete_api_persona")
def delete_api_persona(per_id):
    delete_sql = """
-- delete bits
delete from contacts.urls where persona_id=%(pid)s;
delete from contacts.street_addresses where persona_id=%(pid)s;
delete from contacts.phone_numbers where persona_id=%(pid)s;
delete from contacts.email_addresses where persona_id=%(pid)s;

-- delete tags
delete from contacts.tagpersona where persona_id=%(pid)s;

-- delete the main persona
delete from contacts.personas where id=%(pid)s;
"""

    with app.dbconn() as conn:
        api.sql_void(conn, delete_sql, {"pid": per_id})

        payload = json.dumps({"id": per_id})
        api.sql_void(conn, "notify personas, %(payload)s", {"payload": payload})
        conn.commit()

    return api.Results().json_out()


@app.get("/api/persona/<per_id>/bit/new", name="get_api_persona_bit_new")
def get_api_persona_bit_new(per_id):
    bittype = request.query.get("bit_type")

    if bittype not in ("urls", "phone_numbers", "street_addresses", "email_addresses"):
        raise api.UserError("invalid-param", "select one of the valid bit types")

    select = """
select bit.*
from contacts./*BIT*/ bit
where false"""

    results = api.Results()
    with app.dbconn() as conn:
        select = select.replace("/*BIT*/", bittype)
        columns, rows = api.sql_tab2(conn, select)

        if bittype == "urls":
            # TODO: after all encrypted and password removed, include that here
            columns = [c for c in columns if c[0] != "password_enc"]

        def default_row(index, row):
            row.id = str(uuid.uuid1())
            row.persona_id = per_id

        rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables["bits", True] = columns, rows
    return results.json_out()


@app.get("/api/persona/<per_id>/bit/<bit_id>", name="get_api_persona_bit")
def get_api_persona_bit(per_id, bit_id):
    bittype = request.query.get("bit_type", None)

    if bittype not in ("urls", "phone_numbers", "street_addresses", "email_addresses"):
        raise api.UserError("invalid-param", "select one of the valid bit types")

    select = """
select bit.*
from contacts./*BIT*/ bit
where bit.id=%(bit_id)s"""

    results = api.Results()
    with app.dbconn() as conn:
        if bittype == None:
            bittype = api.sql_1row(
                conn,
                "select bit_type from contacts.bit where id=%(bit_id)s",
                {"bit_id": bit_id},
            )

        select = select.replace("/*BIT*/", bittype)
        rawdata = api.sql_tab2(conn, select, {"bit_id": bit_id})

        if bittype == "urls":
            # use the key to decrypt the password replacing column password_enc
            # with password
            f = fernet_keyed()

            columns = api.tab2_columns_transform(rawdata[0], remove=["password_enc"])

            def decrypt(oldrow, row):
                if row.password == None and oldrow.password_enc != None:
                    # convert the psycopg2 memoryview to bytes
                    row.password = f.decrypt(oldrow.password_enc.tobytes()).decode(
                        "utf8"
                    )

            rows = api.tab2_rows_transform(rawdata, columns, decrypt)
            # not so raw any more, but that's ok
            rawdata = columns, rows

        results.tables["bit", True] = rawdata

    return results.json_out()


@app.put("/api/persona/<per_id>/bit/<bit_id>", name="put_api_persona_bit")
def put_api_persona_contact_bits(per_id, bit_id):
    base_cols = ["is_primary", "name", "memo"]
    bt_url_cols = ["url", "username", "password"]
    bt_email_cols = ["email"]
    bt_number_cols = ["number"]
    bt_address_cols = ["address1", "address2", "city", "state", "zip", "country"]

    bit = api.table_from_tab2(
        "bit",
        amendments=["id", "persona_id"],
        options=[
            *base_cols,
            *bt_url_cols,
            *bt_email_cols,
            *bt_number_cols,
            *bt_address_cols,
        ],
    )

    # detect the bit-type from the given columns
    if "url" in bit.DataRow.__slots__:
        bittype = "urls"
    elif "email" in bit.DataRow.__slots__:
        bittype = "email_addresses"
    elif "number" in bit.DataRow.__slots__:
        bittype = "phone_numbers"
    elif "address1" in bit.DataRow.__slots__:
        bittype = "street_addresses"

    # validate that no invalid columns (or ambiguous) are given for this bit
    # type
    slots = bit.DataRow.__slots__[:]
    slots.remove("id")
    slots.remove("persona_id")
    allowed = {
        "urls": base_cols + bt_url_cols,
        "email_addresses": base_cols + bt_email_cols,
        "phone_numbers": base_cols + bt_number_cols,
        "street_addresses": base_cols + bt_address_cols,
    }[bittype]
    if len(set(slots).difference(allowed)) > 0:
        extra = set(slots).difference(allowed)
        raise api.UserError(
            "invalid-structure",
            f"The column(s) {', '.join(extra)} are not allows in bit type {bittype}",
        )

    if len(bit.rows) != 1:
        raise api.UserError(
            "invalid-structure", "write exactly one bit in this end-point"
        )

    for row in bit.rows:
        row.persona_id = per_id
        row.id = bit_id

    with app.dbconn() as conn:
        if bittype == "urls" and "password" in bit.DataRow.__slots__:
            # use the key to encrypt the password
            f = fernet_keyed()

            # replace column password with password_enc
            # null password for now (soon that column with be deleted)
            columns = []
            to_copy = []
            for c in bit.DataRow.__slots__:
                columns.append(c)
                to_copy.append(c)
                if c == "password":
                    columns.append("password_enc")

            tt = rtlib.simple_table(columns)
            for row in bit.rows:
                with tt.adding_row() as r2:
                    for a in to_copy:
                        setattr(r2, a, getattr(row, a))
                    if row.password != None:
                        r2.password_enc = f.encrypt(row.password.encode("utf8"))
                    r2.password = None

            bit = tt

        with api.writeblock(conn) as w:
            w.upsert_rows(f"contacts.{bittype}", bit)
        conn.commit()

    return api.Results().json_out()


@app.delete("/api/persona/<per_id>/bit/<bit_id>", name="delete_api_persona_bit")
def delete_api_persona_bit(per_id, bit_id):
    delete_sql = """
delete from contacts.urls where persona_id=%(pid)s and id=%(bid)s;
delete from contacts.street_addresses where persona_id=%(pid)s and id=%(bid)s;
delete from contacts.phone_numbers where persona_id=%(pid)s and id=%(bid)s;
delete from contacts.email_addresses where persona_id=%(pid)s and id=%(bid)s;
"""

    with app.dbconn() as conn:
        api.sql_void(conn, delete_sql, {"pid": per_id, "bid": bit_id})
        conn.commit()

    return api.Results().json_out()
