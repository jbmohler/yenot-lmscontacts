import uuid
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/tags/list', name='get_api_tags_list', \
        report_title='Tag List')
def get_api_tags_list():
    select = r"""
select tags.name,
    tags.id,
    concat_ws(E'\u001C', tpar4.name, tpar3.name, tpar2.name, tpar1.name, tags.name) as path_name
from contacts.tags
left outer join contacts.tags tpar1 on tpar1.id=tags.parent_id
left outer join contacts.tags tpar2 on tpar2.id=tpar1.parent_id
left outer join contacts.tags tpar3 on tpar3.id=tpar2.parent_id
left outer join contacts.tags tpar4 on tpar4.id=tpar3.parent_id
order by path_name
"""

    params = {}

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.lms_contacts_tag.surrogate(),
                name=api.cgen.lms_contacts_tag.name(url_key='id', represents=True))
        results.tables['tags', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

def _get_api_tag(a_id=None, newrow=False):
    select = """
select *
from contacts.tags
where /*WHERE*/"""

    wheres = []
    params = {}
    if a_id != None:
        params['i'] = a_id
        wheres.append("tags.id=%(i)s")
    if newrow:
        wheres.append("False")

    assert len(wheres) == 1
    select = select.replace("/*WHERE*/", wheres[0])

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select, params)

        if newrow:
            def default_row(index, row):
                row.id = str(uuid.uuid1())
            rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables['tag', True] = columns, rows
    return results

@app.get('/api/tag/<a_id>', name='get_api_tag')
def get_api_tag(a_id):
    results = _get_api_tag(a_id)
    return results.json_out()

@app.get('/api/tag/new', name='get_api_tag_new')
def get_api_tag_new():
    results = _get_api_tag(newrow=True)
    results.keys['new_row'] = True
    return results.json_out()

@app.put('/api/tag/<acnt_id>', name='put_api_tag')
def put_api_tag(acnt_id):
    acc = api.table_from_tab2('tag')

    if len(acc.rows) != 1 or acc.rows[0].id != acnt_id:
        raise api.UserError('invalid-input', 'There must be exactly one row and it must match the url.')

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('contacts.tags', acc)
        conn.commit()

    return api.Results().json_out()
