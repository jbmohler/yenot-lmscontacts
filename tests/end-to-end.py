import os
import sys
import time
import concurrent.futures as futures
import rtlib
import yenot.client as yclient
import yenot.tests

TEST_DATABASE = 'yenot_e2e_test'

def test_url(dbname):
    if 'YENOT_DB_URL' in os.environ:
        return os.environ['YENOT_DB_URL']
    # Fall back to local unix socket.  This is the url for unix domain socket.
    return 'postgresql:///{}'.format(dbname)

def init_database(dburl):
    r = os.system('{} ../yenot/scripts/init-database.py {} --full-recreate \
            --ddl-script=schema/contacts.sql \
            --module=lcserver'.format(sys.executable, dburl))
    if r != 0:
        print('error exit')
        sys.exit(r)

def test_crud_personas(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        content = client.get('api/persona/new')
        pertable = content.named_table('persona')
        perrow = pertable.rows[0]
        perrow.l_name = 'Sparrow'
        perrow.f_name = 'Jack'

        client.put('api/persona/{}', perrow.id, files={'persona': pertable.as_http_post_file()})

        content = client.get('api/persona/new')
        pertable = content.named_table('persona')
        perrow = pertable.rows[0]
        perrow.l_name = 'Barbossa'
        perrow.f_name = 'Hector'
        hector_id = perrow.id

        client.put('api/persona/{}', perrow.id, files={'persona': pertable.as_http_post_file()})

        content = client.get('api/personas/list')
        found = [row for row in content.main_table().rows if row.l_name == 'Sparrow']
        assert len(found) == 1

        client.delete('api/persona/{}', found[0].id)
        content = client.get('api/personas/list')
        found = [row for row in content.main_table().rows if row.l_name == 'Sparrow']
        assert len(found) == 0

        content = client.get('api/persona/{}', hector_id)
        per = content.named_table('persona')
        assert per.rows[0].l_name == 'Barbossa'

        session.close()

def test_basic_lists(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        client.get('api/personas/list')
        client.get('api/tags/list')

        session.close()

if __name__ == '__main__':
    srvparams = {
            'dburl': test_url(TEST_DATABASE),
            'modules': ['lcserver']}

    init_database(test_url(TEST_DATABASE))
    #test_crud_tags(srvparams)
    test_crud_personas(srvparams)
    test_basic_lists(srvparams)
