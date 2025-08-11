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

--hetero database query with predict pushdown
create or replace function hetero_ppd_query(query text)
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


    def extract_fields(json,meta,alias,aux,unknown):
        if isinstance(json,list):
            for i in range(len(json)):
                extract_fields(json[i],meta,alias,aux,unknown)
        elif isinstance(json,dict):
            if "literal" not in json:
                keys = list(json.keys())
                for i in range(len(keys)):
                    extract_fields(json[keys[i]],meta,alias,aux,unknown)
        elif isinstance(json,str):
            if "." in json and alias != None and json[0:len(alias)] == alias and json[len(alias)+1:] in meta:
                aux.append(json[len(alias)+1:])
            elif json in meta:
                aux.append(json)
            else:
                unknown.append(json)


    def is_scalar(json,meta,alias):
        fields = []
        others = []
        extract_fields(json=json,meta=meta,alias=alias,aux=fields,unknown=others)
        return True if len(fields) == 0 and len(others) == 0 else False


    def is_single_field_expr(json,meta,alias):
        fields = []
        others = []
        extract_fields(json=json,meta=meta,alias=alias,aux=fields,unknown=others)
        return True if len(set(fields)) == 1 and len(others) == 0 else False


    def predicate_pushdown(json,meta,alias,cond_dict):
        key = list(json.keys())[0]
        if key in ["and","or"]:
            for i in range(len(json[key])):
                if key not in cond_dict:
                    cond_dict[key] = [{}]
                qual_num = len(cond_dict[key])
                if cond_dict[key][qual_num-1]:
                    cond_dict[key].append({})
                    qual_num += 1
                predicate_pushdown(json[key][i],meta,alias,cond_dict[key][qual_num-1])
        else:
            if key in ["gte","lte","lt","gt","eq","neq","in","nin","like","not_like"]:
                lhs = json[key][0]
                rhs = json[key][1]
                if is_scalar(json=lhs,meta=meta,alias=alias) == True and is_single_field_expr(json=rhs,meta=meta,alias=alias) == True:
                    cond_dict[key] = json[key]
                elif is_scalar(json=rhs,meta=meta,alias=alias) == True and is_single_field_expr(json=lhs,meta=meta,alias=alias) == True:
                    cond_dict[key] = json[key]
            elif key in ["missing","exists"]:
                field = json[key]
                if ("." in field and alias != None and field[0:len(alias)] == alias and field[len(alias)+1:] in meta):
                    cond_dict[key] = json[key]
                elif field in meta:
                    cond_dict[key] = json[key]
            else:
                predicate_pushdown(json[key],meta,alias,cond_dict)


    def extract_ordnf(json):
        dnf = True
        for i in range(0,len(json["or"])):
            if 'and' in json["or"][i]:
                for j in range(0,len(json["or"][i]["and"])):
                    if not json["or"][i]["and"][j]:
                        del json["or"][i]["and"][j]
        for i in range(0,len(json["or"])):
            if not ('and' in json["or"][i] and len(json["or"][i]["and"]) > 0):
                dnf = False
                break
        if dnf:
            json["and"] = {"or":[json["or"][i]["and"][0] if len(json["or"][i]["and"]) == 1 else json["or"][i] for i in range(0,len(json["or"]))]}
            del json["or"]


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
        plpy.info('table_name:', detail=tab_name)
        plpy.info('dblink_name:', detail=dblink_name)
        alias = eval('tree' + path[0:len(path)-9] + '["name"]') if path.endswith('["value"]') else '';
        meta = hetero_meta(dblink=dblink_name,relation=tab_name)

        if path.rfind('["value"]["from"]') > 0:
            sub_query_path = path[0:path.rfind('["value"]["from"]')+9]
        elif path.rfind('[0]["from"]') > 0:
            sub_query_path = path[0:path.rfind('[0]["from"]')+3]
        elif path.rfind('[1]["from"]') > 0:
            sub_query_path = path[0:path.rfind('[1]["from"]')+3]
        elif path.rfind('["exists"]["from"]') > 0:
            sub_query_path = path[0:path.rfind('["exists"]["from"]')+10]
        else:
            sub_query_path = None

        if sub_query_path == None:
            sub_query_tree = tree
        else:
            sub_query_tree = eval('tree' + sub_query_path)

        plpy.info('orig_query_tree:', detail=json.dumps(tree))
        plpy.info('sub_query_tree:', detail=json.dumps(sub_query_tree))
        plpy.info('meta:', detail=alias + ' (' + ','.join([m[0] + ' ' + m[1] for m in meta]) + ')')

        cond_dict = {}
        if 'where' in sub_query_tree:
            predicate_pushdown(json=sub_query_tree["where"],meta=[m[0] for m in meta],alias=alias,cond_dict=cond_dict)
            if 'and' in cond_dict:
                for i in range(0,len(cond_dict["and"])):
                    if 'or' in cond_dict["and"][i]:
                        extract_ordnf(cond_dict["and"][i])
            if 'or' in cond_dict:
                extract_ordnf(cond_dict)

        if isinstance(sub_query_tree["from"],list):
            tab_pos = int(path[path.rfind('["from"]')+9:path.find(']',path.rfind('["from"]')+9)])
            for i in range(1,len(sub_query_tree["from"])):
                if 'on' in sub_query_tree["from"][i]:
                    join_pred = {}
                    if tab_pos == i:
                        if 'left join' in sub_query_tree["from"][i] or 'left outer join' in sub_query_tree["from"][i] or 'inner join' in sub_query_tree["from"][i]:
                            predicate_pushdown(json=sub_query_tree["from"][i]["on"],meta=[m[0] for m in meta],alias=alias,cond_dict=join_pred)
                    else:
                        if 'right join' in sub_query_tree["from"][i] or 'right outer join' in sub_query_tree["from"][i] or 'inner join' in sub_query_tree["from"][i]:
                            predicate_pushdown(json=sub_query_tree["from"][i]["on"],meta=[m[0] for m in meta],alias=alias,cond_dict=join_pred)
                    if 'and' in join_pred:
                        for i in range(0,len(join_pred["and"])):
                            if 'or' in join_pred["and"][i]:
                                extract_ordnf(join_pred["and"][i])
                    if 'or' in join_pred:
                        extract_ordnf(join_pred)
                    if 'and' in join_pred:
                        if 'and' in cond_dict:
                            cond_dict["and"].extend(join_pred["and"])
                        else:
                            cond_dict["and"] = join_pred["and"]
            ppd_tree = {"select":"*"}
            if not alias:
                ppd_tree["from"] = tab_name
            else:
                ppd_tree["from"] = {"value":tab_name,"name":alias}
            if 'and' in cond_dict and len(cond_dict["and"]) == 1:
                ppd_tree["where"] = cond_dict["and"][0]
            else:
                ppd_tree["where"] = cond_dict
            remote_sql = format(ppd_tree).replace(chr(39),chr(39)+chr(39))
        else:
            if not alias:
                sub_query_tree["from"] = tab_name
            else:
                sub_query_tree["from"]["value"] = tab_name
            sub_query_tree["select"] = "*"
            remote_sql = format(sub_query_tree).replace(chr(39),chr(39)+chr(39))

        if not alias:
            exec('tree' + path + ' = {"value":"","name": ""}')
            exec('tree' + path + '["value"] = ' + plpy.quote_ident('hetero_heap_tuple(\'' + remote_sql + '\',\'' + dblink_name + '\')'))
            exec('tree' + path + '["name"] = ' + plpy.quote_literal('(' + ','.join([m[0] + ' ' + m[1] for m in meta]) + ')'))
        else:
            exec('tree' + path + ' = ' + plpy.quote_ident('hetero_heap_tuple(\'' + remote_sql + '\',\'' + dblink_name + '\')'))
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
