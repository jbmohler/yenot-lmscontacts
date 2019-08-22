import uuid
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

def get_api_personas_list_prompts():
    return api.PromptList(
            frag=api.cgen.basic(label='Search'),
            __order__=['frag'])

@app.get('/api/personas/list', name='get_api_personas_list', \
        report_prompts=get_api_personas_list_prompts,
        report_title='Contact List')
def get_api_personas_list():
    frag = request.query.get('frag', None)
    tag = request.query.get('tag_id', None)

    select = """
select id, l_name, f_name, title
from contacts.personas
where /*WHERE*/
"""

    params = {}
    wheres = []
    if frag != None and frag != '':
        params['frag'] = api.sanitize_fragment(frag)
        wheres.append("personas.l_name like %(frag)s")
    if tag != None:
        params['tag'] = '%{}%'.format(frag)
        wheres.append("(select count(*) from contacts.tagpersona where tag_id=%(tag)s and persona_id=persona.id)>0")

    if len(wheres) == 0:
        wheres.append("True")
    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.lms_personas_persona.surrogate(),
                l_name=api.cgen.lms_personas_persona.name(url_key='id', represents=True))
        results.tables['personas', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

def get_api_personas_search_all_prompts():
    return api.PromptList(
            frag=api.cgen.basic(label='Search'),
            __order__=['frag'])

@app.get('/api/personas/search-all', name='get_api_personas_search_all', \
        report_prompts=get_api_personas_search_all_prompts,
        report_title='Contact List')
def get_api_personas_search_all():
    frag = request.query.get('frag', None)

    select = """
(
    select personas.id, null::uuid as bit_id, l_name, f_name, title
    from contacts.personas
    where /*WHERE*/
)union(
    select personas.id, bit.id as bit_id, l_name, f_name, title
    from contacts.personas
    join contacts.email_addresses bit on bit.persona_id=personas.id
    where /*BIT_WHERE*/
)union(
    select personas.id, bit.id as bit_id, l_name, f_name, title
    from contacts.personas
    join contacts.phone_numbers bit on bit.persona_id=personas.id
    where /*BIT_WHERE*/
)union(
    select personas.id, bit.id as bit_id, l_name, f_name, title
    from contacts.personas
    join contacts.street_addresses bit on bit.persona_id=personas.id
    where /*BIT_WHERE*/
)union(
    select personas.id, bit.id as bit_id, l_name, f_name, title
    from contacts.personas
    join contacts.urls bit on bit.persona_id=personas.id
    where /*BIT_WHERE*/
)
"""

    params = {}
    params['frag'] = api.sanitize_fragment(frag)
    wheres = []
    wheres.append("personas.l_name ilike %(frag)s or personas.f_name ilike %(frag)s or personas.memo ilike %(frag)s")
    bit_wheres = []
    bit_wheres.append("bit.memo ilike %(frag)s or bit.name ilike %(frag)s")

    select = select.replace("/*WHERE*/", " and ".join(wheres))
    select = select.replace("/*BIT_WHERE*/", " and ".join(bit_wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.lms_personas_persona.surrogate(),
                l_name=api.cgen.lms_personas_persona.name(url_key='id', represents=True))
        results.tables['personas', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

def _get_api_persona(a_id=None, newrow=False):
    select = """
select *
from contacts.personas
where /*WHERE*/"""

    select_bit1 = """
select *
from contacts.phone_numbers bit
where /*BWHERE*/"""

    wheres = []
    bwheres = []
    params = {}
    if a_id != None:
        params['i'] = a_id
        wheres.append("personas.id=%(i)s")
        bwheres.append("bit.persona_id=%(i)s")
    if newrow:
        wheres.append("False")
        bwheres.append("False")

    assert len(wheres) == 1
    select = select.replace("/*WHERE*/", wheres[0])
    select_bit1 = select_bit1.replace("/*BWHERE*/", bwheres[0])

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select, params)

        if newrow:
            def default_row(index, row):
                row.id = str(uuid.uuid1())
            rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables['persona', True] = columns, rows
        results.tables['phone_numbers'] = api.sql_tab2(conn, select_bit1, params)
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

@app.put('/api/persona/<acnt_id>', name='put_api_persona')
def put_api_persona(acnt_id):
    acc = api.table_from_tab2('persona')

    if len(acc.rows) != 1 or acc.rows[0].id != acnt_id:
        raise api.UserError('invalid-input', 'There must be exactly one row and it must match the url.')

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('contacts.personas', acc)
        conn.commit()

    return api.Results().json_out()

@app.post('/api/persona/<acnt_id>/contact-bits', name='post_api_persona_contact_bits')
def post_api_persona_contact_bits(acnt_id):
    acc = api.table_from_tab2('phone_numbers', amendments=['id', 'persona_id'])

    for row in acc.rows:
        row.id = None
        row.persona_id = acnt_id

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('contacts.phone_numbers', acc)
        conn.commit()

    return api.Results().json_out()
