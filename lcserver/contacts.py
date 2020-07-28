import uuid
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

def get_api_personas_list_prompts():
    return api.PromptList(
            frag=api.cgen.basic(label='Search'),
            tag_id=api.cgen.contact_tag.id(label='Tag'),
            __order__=['frag', 'tag_id'])

@app.get('/api/personas/list', name='get_api_personas_list', \
        report_prompts=get_api_personas_list_prompts,
        report_title='Contact List')
def get_api_personas_list():
    frag = request.query.get('frag', None)
    tag = request.query.get('tag_id', None)

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
    if frag != None and frag != '':
        params['frag'] = api.sanitize_fts(frag)
        wheres.append("fts.fts_search @@ to_tsquery(%(frag)s)")
    if tag not in ['', None]:
        params['tag'] = tag
        wheres.append("(select count(*) from contacts.tagpersona where tag_id=%(tag)s and persona_id=personas.id)>0")

    if len(wheres) == 0:
        wheres.append("True")
    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.lms_personas_persona.surrogate(),
                entity_name=api.cgen.lms_personas_persona.name(url_key='id', represents=True),
                l_name=api.cgen.lms_personas_persona.name(hidden=True),
                f_name=api.cgen.lms_personas_persona.name(hidden=True),
                title=api.cgen.lms_personas_persona.name(hidden=True))
        results.tables['personas', True] = api.sql_tab2(conn, select, params, cm)
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
        params['i'] = a_id
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

        results.tables['persona', True] = columns, rows
        results.tables['bits'] = api.sql_tab2(conn, select_bits, params)
    return results

@app.get('/api/persona/<a_id>', name='get_api_persona')
def get_api_persona(a_id):
    results = _get_api_persona(a_id)
    return results.json_out()

@app.get('/api/persona/new', name='get_api_persona_new')
def get_api_persona_new():
    results = _get_api_persona(newrow=True)
    results.keys['new_row'] = True
    return results.json_out()

@app.put('/api/persona/<per_id>', name='put_api_persona')
def put_api_persona(per_id):
    persona = api.table_from_tab2('persona', amendments=['id'], options=['l_name', 'f_name', 'title', 'organization', 'memo', 'anniversary', 'birthday'])
    try:
        tagdeltas = api.table_from_tab2('tagdeltas', required=['tags_add', 'tags_remove'])
    except KeyError:
        tagdeltas = None

    if len(persona.rows) != 1 or persona.rows[0].id != per_id:
        raise api.UserError('invalid-input', 'There must be exactly one row and it must match the url.')
    if tagdeltas != None and len(tagdeltas.rows) != 1:
        raise api.UserError('invalid-input', 'There must be exactly one tagdeltas row.')

    insert_adds = """
insert into contacts.tagpersona (tag_id, persona_id)
select unnest(%(adds)s)::uuid, %(per)s"""
# TODO:  need unique constraint
#on conflict (tag_id, persona_id) do nothing"""

    delete_removes = """
delete from contacts.tagpersona
where tag_id in %(removes)s and persona_id=%(per)s"""

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('contacts.personas', persona)
        if tagdeltas:
            if tagdeltas.rows[0].tags_add:
                api.sql_void(conn, insert_adds, {"adds": tagdeltas.rows[0].tags_add, "per": per_id})
            if tagdeltas.rows[0].tags_remove:
                api.sql_void(conn, delete_removes, {"removes": tuple(tagdeltas.rows[0].tags_remove), "per": per_id})
        conn.commit()

    return api.Results().json_out()

@app.delete('/api/persona/<per_id>', name='delete_api_persona')
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
        api.sql_void(conn, delete_sql, {'pid': per_id})
        conn.commit()

    return api.Results().json_out()

@app.get('/api/persona/<per_id>/bit/new', name='get_api_persona_bit_new')
def get_api_persona_bit_new(per_id):
    bittype = request.query.get('bit_type')

    if bittype not in ('urls', 'phone_numbers', 'street_addresses', 'email_addresses'):
        raise api.UserError('invalid-param', 'select one of the valid bit types')

    select = """
select bit.*
from contacts./*BIT*/ bit
where false"""

    results = api.Results()
    with app.dbconn() as conn:
        select = select.replace("/*BIT*/", bittype)
        columns, rows = api.sql_tab2(conn, select)

        def default_row(index, row):
            row.id = str(uuid.uuid1())
            row.persona_id = per_id
        rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables['bits', True] = columns, rows
    return results.json_out()

@app.get('/api/persona/<per_id>/bit/<bit_id>', name='get_api_persona_bit')
def get_api_persona_bit(per_id, bit_id):
    bittype = request.query.get('bit_type', None)

    if bittype not in ('urls', 'phone_numbers', 'street_addresses', 'email_addresses'):
        raise api.UserError('invalid-param', 'select one of the valid bit types')

    select = """
select bit.*
from contacts./*BIT*/ bit
where bit.id=%(bit_id)s"""

    results = api.Results()
    with app.dbconn() as conn:
        if bittype == None:
            bittype = api.sql_1row(conn, "select bit_type from contacts.bit where id=%(bit_id)s", {"bit_id": bit_id})

        select = select.replace("/*BIT*/", bittype)
        results.tables['bit', True] = api.sql_tab2(conn, select, {"bit_id": bit_id})
    return results.json_out()

@app.put('/api/persona/<per_id>/bit/<bit_id>', name='put_api_persona_bit')
def put_api_persona_contact_bits(per_id, bit_id):
    bit = api.table_from_tab2('bit', amendments=['id', 'persona_id'], 
            options=['is_primary', 'name', 'memo', 
                'url', 'username', 'password', 
                'email',
                'number',
                'address1', 'address2', 'city', 'state', 'zip', 'country'])

    if 'url' in bit.DataRow.__slots__:
        bittype = 'urls'
    elif 'email' in bit.DataRow.__slots__:
        bittype = 'email_addresses'
    elif 'address1' in bit.DataRow.__slots__:
        bittype = 'street_addresses'
    elif 'number' in bit.DataRow.__slots__:
        bittype = 'phone_numbers'

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('contacts.{}'.format(bittype), bit)
        conn.commit()

    return api.Results().json_out()

@app.delete('/api/persona/<per_id>/bit/<bit_id>', name='delete_api_persona_bit')
def delete_api_persona_bit(per_id, bit_id):
    delete_sql = """
delete from contacts.urls where persona_id=%(pid)s and id=%(bid)s;
delete from contacts.street_addresses where persona_id=%(pid)s and id=%(bid)s;
delete from contacts.phone_numbers where persona_id=%(pid)s and id=%(bid)s;
delete from contacts.email_addresses where persona_id=%(pid)s and id=%(bid)s;
"""

    with app.dbconn() as conn:
        api.sql_void(conn, delete_sql, {'pid': per_id, 'bid': bit_id})
        conn.commit()

    return api.Results().json_out()

@app.post('/api/persona/<per_id>/contact-bits', name='post_api_persona_contact_bits')
def post_api_persona_contact_bits(per_id):
    acc = api.table_from_tab2('phone_numbers', amendments=['id', 'persona_id'])

    for row in acc.rows:
        row.id = None
        row.persona_id = per_id

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('contacts.phone_numbers', acc)
        conn.commit()

    return api.Results().json_out()
