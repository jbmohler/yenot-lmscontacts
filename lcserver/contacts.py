import os
import uuid
import json
import cryptography.fernet
import rtlib
import yenot.backend.api as api

app = api.get_global_app()


def _raise_unmatched_share(conn, persona_id):
    select = """
select count(*)
from contacts.persona_shares pshare
where pshare.persona_id=%(pid)s and pshare.user_id=%(uid)s
"""

    active = api.active_user(conn)
    count = api.sql_1row(conn, select, {"pid": persona_id, "uid": active.id})

    if count == 0:
        raise api.UserError(
            "user-not-authorized",
            "This persona is not shared (or owned) by the active user.",
        )


def _raise_unmatched_owner(conn, persona_id, allow_new=False):
    active = api.active_user(conn)
    owner_id = api.sql_1row(
        conn,
        "select owner_id from contacts.personas where id=%(pid)s",
        {"pid": persona_id},
    )

    if owner_id is None and allow_new:
        return

    if owner_id != active.id:
        raise api.UserError("user-not-owner", "Only the owner may edit this persona.")


def fernet_keyed():
    def _fernet(envkey):
        key = os.environ[envkey]
        return cryptography.fernet.Fernet(key.encode("ascii"))

    basekey = "LMS_CONTACTS_KEY"
    fernetbase = _fernet(basekey)
    rotated = [f"{basekey}_ROTATE{i+1}" for i in range(3)]
    ferns = [_fernet(rkey) for rkey in rotated if rkey in os.environ]

    print(f"Returning MultiFernet with {len(ferns)} rotated keys")

    return cryptography.fernet.MultiFernet([fernetbase, *ferns])


@app.get("/api/personas/owner-list", name="get_api_personas_owner_list")
def get_api_personas_owner_list(request):
    # return users that have access to contacts
    select = """
select id, coalesce(full_name, username) as name
from users
join lateral (
    select count(*)
    from userroles
    join roleactivities on roleactivities.roleid=userroles.roleid
    where userroles.userid=users.id
        and roleactivities.activityid=(select id from activities where act_name='get_api_personas_list')
    ) has_contacts on has_contacts.count>0
"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables["owners", True] = api.sql_tab2(conn, select)

    return results.json_out()


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
def get_api_personas_list(request):
    frag = request.query.get("frag", None)
    included = request.query.get("included", None)
    tag = request.query.get("tag_id", None)

    select = """
select personas.id,
    personas.entity_name,
    personas.corporate_entity,
    personas.l_name, personas.f_name, personas.title, personas.organization,
    coalesce(users.full_name, users.username) as owner
from contacts.personas_calc as personas
join contacts.persona_shares pshare on pshare.persona_id=personas.id
join users on users.id=personas.owner_id
where /*WHERE*/
order by personas.entity_name
"""

    params = {}
    wheres = []
    if frag not in ["", None] and included not in ["", None]:
        params["frag"] = api.sanitize_fts(frag)
        params["idlist"] = tuple(included.split(";"))
        wheres.append(
            "(personas.fts_search @@ to_tsquery(%(frag)s) or personas.id in %(idlist)s)"
        )
    elif frag in ["", None] and included not in ["", None]:
        params["idlist"] = tuple(included.split(";"))
        wheres.append("personas.id in %(idlist)s")
    elif frag not in ["", None] and included in ["", None]:
        params["frag"] = api.sanitize_fts(frag)
        wheres.append("personas.fts_search @@ to_tsquery(%(frag)s)")
    if tag not in ["", None]:
        params["tag"] = tag
        wheres.append(
            "(select count(*) from contacts.tagpersona where tag_id=%(tag)s and persona_id=personas.id)>0"
        )

    wheres.append("pshare.user_id=%(uid)s")

    if len(wheres) == 0:
        wheres.append("True")
    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        active = api.active_user(conn)
        params["uid"] = active.id

        cm = api.ColumnMap(
            id=api.cgen.lms_personas_persona.surrogate(),
            corporate_entity=api.cgen.auto(hidden=True),
            entity_name=api.cgen.lms_personas_persona.name(
                url_key="id", represents=True
            ),
            l_name=api.cgen.auto(hidden=True),
            f_name=api.cgen.auto(hidden=True),
            title=api.cgen.auto(hidden=True),
        )
        results.tables["personas", True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()


@app.put("/api/personas/poll-changes", name="put_api_personas_poll_changes")
def put_api_personas_poll_changes(request):
    return api.start_listener(request, "personas")


@app.get("/api/personas/poll-changes", name="get_api_personas_poll_changes")
def get_api_personas_poll_changes(request):
    return api.poll_listener(request, "personas")


def _get_api_persona(a_id=None, newrow=False):
    select = """
select personas.id, 
    personas.entity_name,
    personas.corporate_entity,
    personas.l_name, personas.f_name, personas.title, personas.organization,
    personas.memo,
    personas.birthday, personas.anniversary,
    personas.owner_id,
    coalesce(users.full_name, users.username) as owner_name,
    shares.share_refs,
    taglist.tag_ids
from contacts.personas_calc personas
join users on users.id=personas.owner_id
join lateral (
    select array_agg(tagpersona.tag_id::text) as tag_ids
    from contacts.tagpersona
    where tagpersona.persona_id=personas.id) taglist on true
join lateral (
    select json_agg(json_build_object(
        'id', pshare.user_id,
        'name', coalesce(u2.full_name, u2.username)
        )) as share_refs
    from contacts.persona_shares pshare
    join users u2 on u2.id=pshare.user_id
    where pshare.persona_id=personas.id) shares on true
where /*WHERE*/"""

    select_bits = """
select id, persona_id, bit_type, 
    name, memo, is_primary,
    bit_data
from contacts.bits
where /*BWHERE*/
order by bit_sequence"""

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
        if not newrow:
            _raise_unmatched_share(conn, a_id)

        cm = api.ColumnMap(
            entity_name=api.cgen.auto(skip_write=True),
            owner_name=api.cgen.auto(skip_write=True),
            share_refs=api.cgen.auto(skip_write=True),
            tag_ids=api.cgen.auto(skip_write=True),
        )
        columns, rows = api.sql_tab2(conn, select, params, cm)

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
            if "password_enc" in oldrow.bit_data:
                # the dictionary from postgres comes through with password_enc as a string
                penc = oldrow.bit_data["password_enc"]
                if penc is None:
                    row.bit_data["password"] = None
                else:
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
        amendments=["id", "owner_id"],
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
    tagdeltas = api.table_from_tab2(
        "tagdeltas", default_missing="none", required=["tags_add", "tags_remove"]
    )

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

    insert_share = """
insert into contacts.persona_shares (persona_id, user_id)
values (%(pid)s, %(uid)s);
"""

    with app.dbconn() as conn:
        _raise_unmatched_owner(conn, persona.rows[0].id, allow_new=True)

        share_to_owner = None
        if getattr(persona.rows[0], "owner_id", None) is None:
            active = api.active_user(conn)
            persona.rows[0].owner_id = active.id

            share_to_owner = {"pid": persona.rows[0].id, "uid": active.id}

        with api.writeblock(conn) as w:
            w.upsert_rows("contacts.personas", persona)

        if share_to_owner:
            api.sql_void(conn, insert_share, share_to_owner)

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
        api.notify_listener(conn, "personas", payload)
        conn.commit()

    return api.Results().json_out()


@app.put("/api/persona/<per_id>/reshare", name="put_api_persona_reshare")
def put_persona_reshare(request, per_id):
    personas = api.table_from_tab2(
        "persona", required=["id", "shares"], matrix=["shares"]
    )

    with app.dbconn() as conn:
        _raise_unmatched_owner(conn, persona.rows[0].id)

        with api.writeblock(conn) as w:
            w.update_rows(
                "contacts.personas",
                personas,
                matrix={"shares": "contacts.persona_shares"},
            )
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
        _raise_unmatched_owner(conn, per_id)

        api.sql_void(conn, delete_sql, {"pid": per_id})

        payload = json.dumps({"id": per_id})
        api.notify_listener(conn, "personas", payload)
        conn.commit()

    return api.Results().json_out()


@app.put("/api/persona/<per_id>/reown", name="put_api_persona_reown")
def put_api_persona_reown(request, per_id):
    owner_id = request.forms.get("owner_id")

    upsert_new = """
insert into contacts.persona_shares (persona_id, user_id)
values (%(pid)s, %(new_uid)s)
on conflict do nothing;"""

    update = """
update contacts.personas set owner_id=%(new_uid)s where id=%(pid)s;
"""

    with app.dbconn() as conn:
        _raise_unmatched_owner(conn, per_id)

        # This persona will be shared with the active user but owned by the new
        # `owner_id`.   The persona need not be shared with the new owner
        # previously.

        params = {"pid": per_id, "new_uid": owner_id}

        api.sql_void(conn, upsert_new, params)
        api.sql_void(conn, update, params)
        conn.commit()

    return api.Results().json_out()


@app.get("/api/persona/<per_id>/bit/new", name="get_api_persona_bit_new")
def get_api_persona_bit_new(request, per_id):
    bittype = request.query.get("bit_type")

    if bittype not in ("urls", "phone_numbers", "street_addresses", "email_addresses"):
        raise api.UserError("invalid-param", "select one of the valid bit types")

    select = """
select bit.*
from contacts./*BIT*/ bit
where false"""

    results = api.Results()
    with app.dbconn() as conn:
        _raise_unmatched_owner(conn, per_id)

        select = select.replace("/*BIT*/", bittype)
        columns, rows = api.sql_tab2(conn, select)

        if bittype == "urls":
            columns = [
                c for c in columns if c[0] not in ["password_enc", "bit_sequence"]
            ]
            columns.append(("password", None))
        else:
            columns = [c for c in columns if c[0] not in ["bit_sequence"]]

        def default_row(index, row):
            row.id = str(uuid.uuid1())
            row.persona_id = per_id
            row.is_primary = False

        rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables["bit", True] = columns, rows
    return results.json_out()


@app.get("/api/persona/<per_id>/bit/<bit_id>", name="get_api_persona_bit")
def get_api_persona_bit(request, per_id, bit_id):
    bittype = request.query.get("bit_type", None)

    if bittype not in ("urls", "phone_numbers", "street_addresses", "email_addresses"):
        raise api.UserError("invalid-param", "select one of the valid bit types")

    select = """
select bit.*
from contacts./*BIT*/ bit
where bit.id=%(bit_id)s"""

    results = api.Results()
    with app.dbconn() as conn:
        _raise_unmatched_share(conn, per_id)

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

            columns = api.tab2_columns_transform(
                rawdata[0],
                remove=["password_enc", "bit_sequence"],
                insert=[("username", "password")],
            )

            def decrypt(oldrow, row):
                if oldrow.password_enc != None:
                    enc = oldrow.password_enc
                    # convert the psycopg2 memoryview to bytes
                    row.password = f.decrypt(enc.tobytes()).decode("utf8")

            rows = api.tab2_rows_transform(rawdata, columns, decrypt)
            # not so raw any more, but that's ok
            rawdata = columns, rows
        else:
            columns = api.tab2_columns_transform(rawdata[0], remove=["bit_sequence"])

            def nothing(oldrow, row):
                pass

            rows = api.tab2_rows_transform(rawdata, columns, nothing)
            # not so raw any more, but that's ok
            rawdata = columns, rows

        results.tables["bit", True] = rawdata

    return results.json_out()


@app.put("/api/persona/<per_id>/bits/reorder", name="put_api_persona_bits_reorder")
def put_api_persona_bits_reorder(request, per_id):
    # order this with bit_id1 ordered before bit_id2
    bit_id1 = request.forms.get("bit_id1")
    bit_id2 = request.forms.get("bit_id2")

    select = """
select id, bit_type, bit_sequence
from contacts.bits
where persona_id=%(pid)s and id in (%(bid1)s, %(bid2)s);
"""

    update = """
update contacts.{bt} set bit_sequence=%(seq)s
where id=%(bid)s;
"""

    with app.dbconn() as conn:
        _raise_unmatched_owner(conn, per_id)

        params = {"pid": per_id, "bid1": bit_id1, "bid2": bit_id2}
        rows = api.sql_rows(conn, select, params)
        rows.sort(key=lambda x: x.bit_sequence)

        if len(rows) != 2:
            raise api.UserError(
                "invalid-keys",
                "Could not find both bits; possibly deleted by another user",
            )

        bits = {row.id: row for row in rows}

        # TODO use psycopg sql building for sql identifiers
        up1 = update.format(bt=bits[bit_id1].bit_type)
        params = {"seq": rows[0].bit_sequence, "bid": bit_id1}
        api.sql_void(conn, up1, params)

        up1 = update.format(bt=bits[bit_id2].bit_type)
        params = {"seq": rows[1].bit_sequence, "bid": bit_id2}
        api.sql_void(conn, up1, params)

        conn.commit()

    return api.Results().json_out()


@app.put("/api/persona/<per_id>/bit/<bit_id>", name="put_api_persona_bit")
def put_api_persona_bit(per_id, bit_id):
    base_cols = ["is_primary", "name", "memo"]
    bt_url_cols = ["url", "username", "password", "pw_reset_dt", "pw_next_reset_dt"]
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
        _raise_unmatched_owner(conn, per_id)

        if bittype == "urls" and "password" in bit.DataRow.__slots__:
            # use the key to encrypt the password
            f = fernet_keyed()

            # replace column password with password_enc
            # null password for now (soon that column with be deleted)
            columns = []
            to_copy = []
            for c in bit.DataRow.__slots__:
                if c == "password":
                    columns.append("password_enc")
                else:
                    columns.append(c)
                    to_copy.append(c)

            tt = rtlib.simple_table(columns)
            for row in bit.rows:
                with tt.adding_row() as r2:
                    for a in to_copy:
                        setattr(r2, a, getattr(row, a))
                    if row.password != None:
                        r2.password_enc = f.encrypt(row.password.encode("utf8"))

            bit = tt

        with api.writeblock(conn) as w:
            w.upsert_rows(f"contacts.{bittype}", bit)
        conn.commit()

    return api.Results().json_out()


@app.put("/api/persona/<per_id>/bit/<bit_id>/rotate", name="put_api_persona_bit_rotate")
def put_api_persona_contact_bit_rotate(per_id, bit_id):
    # bit_type must be url

    select = """
select id, persona_id, password_enc
from contacts.urls
where id=%(bitid)s and persona_id=%(perid)s
"""
    update = """
update contacts.urls set password_enc=%(newpass)s
where id=%(bitid)s and persona_id=%(perid)s
"""

    with app.dbconn() as conn:
        row = api.sql_1object(conn, select, {"bitid": bit_id, "perid": per_id})

        if row is None:
            raise api.UserError("invalid-key", "No password bit for that id to rotate")

        # use the key to encrypt the password
        f = fernet_keyed()

        newpass = f.rotate(row.password_enc.tobytes())
        api.sql_void(
            conn, update, {"bitid": bit_id, "perid": per_id, "newpass": newpass}
        )
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
        _raise_unmatched_owner(conn, per_id)

        api.sql_void(conn, delete_sql, {"pid": per_id, "bid": bit_id})
        conn.commit()

    return api.Results().json_out()


@app.get("/api/personas/all-bits", name="get_api_personas_all_bits")
def get_api_personas_all_bits(request):
    bittype = request.query.get("bit_type", None)

    if bittype != "urls":
        raise api.UserError("not-implemented", "only urls are supported at this point")

    select = """
select urls.persona_id, urls.id,
    personas.entity_name
from contacts.urls
join contacts.personas_calc personas on personas.id=urls.persona_id
"""

    results = api.Results()
    with app.dbconn() as conn:
        results.tables["contacts"] = api.sql_tab2(conn, select)
    return results.json_out()
