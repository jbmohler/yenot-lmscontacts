import yenot.backend.api as api

app = api.get_global_app()


def get_api_personas_address_list_prompts():
    return api.PromptList(
        tag_id=api.cgen.contact_tag.id(label="Tag"), __order__=["tag_id"]
    )


@app.get(
    "/api/personas/address-list",
    name="get_api_personas_address_list",
    report_prompts=get_api_personas_address_list_prompts,
    report_title="Street Address List",
)
def get_api_personas_list(request):
    tag = request.query.get("tag_id", None)

    select = """
select personas.id, 
    personas.f_name||' '||personas.l_name as name,
    personas.l_name, personas.f_name, 
    array_to_string(
        array[case when sa.name='' then null else sa.name end, 
            case when sa.address1='' then null else sa.address1 end,
            case when sa.address2='' then null else sa.address2 end,
            array_to_string(array[city, state, zip, country], ' ')],
            chr(10)) as street_address
from contacts.personas
left outer join contacts.street_addresses sa on sa.persona_id=personas.id
join contacts.tagpersona on tagpersona.persona_id=personas.id
where tagpersona.tag_id=%(tag)s
order by personas.l_name, personas.f_name
"""

    params = {"tag": tag}

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            id=api.cgen.lms_personas_persona.surrogate(),
            name=api.cgen.lms_personas_persona.name(url_key="id", represents=True),
            l_name=api.cgen.lms_personas_persona.name(hidden=True),
            f_name=api.cgen.lms_personas_persona.name(hidden=True),
            street_address=api.cgen.multiline(),
        )
        results.tables["personas", True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()
