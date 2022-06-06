--remote table query in pg side
create or replace function hetero_heap_tuple(query text, foreign_data_server text)
returns setof record
as $$
    import psycopg2
    import cx_Oracle
    import pymysql
    fds = plpy.execute("select w.fdwname,s.srvoptions,m.umoptions from pg_foreign_server s inner join pg_foreign_data_wrapper w on s.srvfdw = w.oid inner join pg_user_mapping m on s.oid = m.umserver inner join pg_user u on m.umuser = u.usesysid and u.usename = current_user where s.srvname = " + plpy.quote_literal(foreign_data_server))
    if not fds:
        plpy.error('HV000', detail='this dblink is not found in foreign data server')
    if fds[0]["fdwname"] == 'dblink_fdw':
        conn = psycopg2.connect(database=fds[0]["srvoptions"][1][7:], user=fds[0]["umoptions"][0][5:], password=fds[0]["umoptions"][1][9:], host=fds[0]["srvoptions"][0][5:], port=fds[0]["srvoptions"][2][5:])
    elif fds[0]["fdwname"] == 'oracle_fdw':
        conn = cx_Oracle.connect(fds[0]["umoptions"][0][5:],fds[0]["umoptions"][1][9:],fds[0]["srvoptions"][0][9:])
    elif fds[0]["fdwname"] == 'mysql_fdw':
        conn = pymysql.connect(user=fds[0]["umoptions"][0][9:], password=fds[0]["umoptions"][1][9:], host=fds[0]["srvoptions"][0][5:], port=int(fds[0]["srvoptions"][1][5:]))
    else:
        plpy.error('0A000', detail='this database is not supported in foreign data wrapper', hint='sorry,we will support this later')
    cursor = conn.cursor()
    cursor.execute(query)
    while True:
        rows = cursor.fetchmany(100)
        if not rows:
            break
        for row in rows:
            yield row
$$ language plpython3u;

--hetero database query in simple mode (no predict pushdown)
create or replace function hetero_query(query text)
returns setof record
as $$
    import psycopg2
    import cx_Oracle
    import pymysql
    import json
    from mo_sql_parsing import parse
    from mo_sql_parsing import format


    def dblink_table_paths(json,path,path_list):
        if isinstance(json,list):
            for i in range(len(json)):
                tmp_path = path + '[' + str(i) + ']'
                if isinstance(json[i],str):
                    if '@' in json[i]:
                        path_list.append(tmp_path)
                else:
                    dblink_table_paths(json[i],tmp_path,path_list)
        elif isinstance(json,dict):
            keys = list(json.keys())
            for i in range(len(keys)):
                tmp_path = path + '["' + keys[i] + '"]'
                if 'value' == keys[i]:
                    if isinstance(json['value'],str) and '@' in json['value']:
                        path_list.append(tmp_path)
                    else:
                        dblink_table_paths(json['value'],tmp_path,path_list)
                elif 'from' == keys[i]:
                    if isinstance(json['from'],str) and '@' in json['from']:
                        path_list.append(tmp_path)
                    else:
                        dblink_table_paths(json['from'],tmp_path,path_list)
                else:
                    dblink_table_paths(json[keys[i]],tmp_path,path_list)


    def hetero_meta(dblink,relation):
        fds = plpy.execute("select w.fdwname,s.srvoptions,m.umoptions from pg_foreign_server s inner join pg_foreign_data_wrapper w on s.srvfdw = w.oid inner join pg_user_mapping m on s.oid = m.umserver inner join pg_user u on m.umuser = u.usesysid and u.usename = current_user where s.srvname = " + plpy.quote_literal(dblink))
        if not fds:
            plpy.error('HV000', detail='this dblink is not found in foreign data server')
        if fds[0]["fdwname"] == 'dblink_fdw':
            conn = psycopg2.connect(database=fds[0]["srvoptions"][1][7:], user=fds[0]["umoptions"][0][5:], password=fds[0]["umoptions"][1][9:], host=fds[0]["srvoptions"][0][5:], port=fds[0]["srvoptions"][2][5:])
            cursor = conn.cursor()
            cursor.execute("select a.attname as field,t.typname as type,a.attnum as attnum from pg_class c,pg_attribute a left join pg_description b on a.attrelid = b.objoid and a.attnum = b.objsubid ,pg_type t where c.relname = " + plpy.quote_literal(relation) + " and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid order by a.attnum")
            meta = cursor.fetchall()
        elif fds[0]["fdwname"] == 'oracle_fdw':
            conn = cx_Oracle.connect(fds[0]["umoptions"][0][5:],fds[0]["umoptions"][1][9:],fds[0]["srvoptions"][0][9:])
            cursor = conn.cursor()
            cursor.execute("select lower(column_name),lower(data_type),column_id from user_tab_columns where lower(table_name) = " + plpy.quote_literal(relation) + " order by column_id asc")
            meta = cursor.fetchall()
        elif fds[0]["fdwname"] == 'mysql_fdw':
            conn = pymysql.connect(user=fds[0]["umoptions"][0][9:], password=fds[0]["umoptions"][1][9:], host=fds[0]["srvoptions"][0][5:], port=int(fds[0]["srvoptions"][1][5:]))
            cursor = conn.cursor()
            cursor.execute("select lower(column_name),lower(data_type),ordinal_position from information_schema.columns where table_schema = " + plpy.quote_literal(relation[0:relation.index('.')]) + " and table_name = " + plpy.quote_literal(relation[relation.index('.')+1:]) + " order by ordinal_position asc")
            meta = cursor.fetchall()
        else:
            plpy.error('0A000', detail='this database is not supported in foreign data wrapper', hint='sorry,we will support this later')
        cursor.close()
        return meta


    path_list = []
    tree = parse(query)
    dblink_table_paths(json=tree,path='',path_list=path_list)

    for path in path_list:
        orig_tabname = eval('tree' + path)
        tab_name = orig_tabname[0:orig_tabname.index('@')]
        dblink_name = orig_tabname[orig_tabname.index('@')+1:]
        plpy.info('tab_name:', detail=tab_name)
        plpy.info('dblink_name:', detail=dblink_name)
        alias = eval('tree' + path[0:len(path)-9] + '["name"]') if path.endswith('["value"]') else '';
        meta = hetero_meta(dblink=dblink_name,relation=tab_name)

        plpy.info('tree:', detail=json.dumps(tree))
        plpy.info('meta:', detail=alias + ' (' + ','.join([m[0] + ' ' + m[1] for m in meta]) + ')')

        if not alias:
            exec('tree' + path + ' = {"value":"","name": ""}')
            exec('tree' + path + '["value"] = ' + plpy.quote_ident('hetero_heap_tuple(\'' + 'select * from ' + tab_name + '\',\'' + dblink_name + '\')'))
            exec('tree' + path + '["name"] = ' + plpy.quote_literal('(' + ','.join([m[0] + ' ' + m[1] for m in meta]) + ')'))
        else:
            exec('tree' + path + ' = ' + plpy.quote_ident('hetero_heap_tuple(\'' + 'select * from ' + tab_name + '\',\'' + dblink_name + '\')'))
            exec('tree' + path[0:len(path)-9] + '["name"]'  + ' = ' + plpy.quote_literal(alias + ' (' + ','.join([m[0] + ' ' + m[1] for m in meta]) + ')'))

    plpy.info('act_tree:', detail=json.dumps(tree))
    new_sql = format(tree).replace(chr(34),'')
    plpy.info('act_sql:', detail=new_sql)

    cursor = plpy.cursor(new_sql)
    while True:
        rows = cursor.fetch(10)
        if not rows:
            cursor.close()
            break
        for row in rows:
            yield row

$$ language plpython3u;
