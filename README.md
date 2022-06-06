# pgdb_utils
PostgreSQL tools

# Cookbook
# prepare
1. create extension plpython3u; --can't use Python virtual environment
2. pip install psycopg2-binary cx_Oracle pymysql mo_sql_parsing
3. create different dblink
```sql
--create postgres dblink
create extension dblink;

create server to_hr foreign data wrapper dblink_fdw options (host '127.0.0.1', dbname 'hr', port '5432');

create user mapping for postgres server to_hr options (user 'postgres', password 'root');

grant usage on foreign server to_hr to postgres;
--create oracle dblink
create extension oracle_fdw;

create server to_dcept foreign data wrapper oracle_fdw options(dbserver '127.0.0.1:1521/dcept');

grant usage on foreign server to_dcept to postgres;

create user mapping for postgres server to_dcept options(user 'trader',password 'tiger');
--create mysql dblink
create extension mysql_fdw;

create server to_fb foreign data wrapper mysql_fdw options (host '127.0.0.1', port '3306');

create user mapping for postgres server to_fb options (username 'root', password 'root');
```
 Note:we create dblink only to use pg_foreign_server and pg_user_mapping to store remote database connection information
 # Usage
 ```sql
select * from hetero_heap_tuple('select * from regions','to_hr') as r (region_id int,region_name varchar);

select * from hetero_heap_tuple('select * from fb_test.t0 limit 100','to_fb') as t0(id int, cat text, aid int, bid int, cid int, did int, eid int, fid int, gid int, hid int);

select * from hetero_heap_tuple('select * from spot_price','to_dcept') as spot_price(variety varchar, region varchar, order_type varchar, prod_name varchar, prod_area varchar, quoted_price int, ud_range varchar, data_date date);

    select e.fname || '-' || e.lname as full_name
          ,j.job_title 
      from hetero_heap_tuple('select * from emps','to_hr') 
        as e (id int,fname varchar,lname varchar,email varchar,tel varchar,hdate date,job_id varchar,sal numeric,com_pct numeric,mgr_id int,dept_id int)
inner join hetero_heap_tuple('select * from jobs','to_hr') 
        as j (job_id varchar,job_title varchar,min_sal numeric,max_sal numeric)
        on e.job_id = j.job_id;

select * from hetero_query('select * from fb_test.t0@to_fb limit 100') as t0(id int, cat text, aid int, bid int, cid int, did int, eid int, fid int, gid int, hid int);

select * from hetero_query('select b.* from jobs b inner join emps@to_hr e on b.job_id = b.job_id') as j (job_id varchar,job_title varchar,min_sal numeric,max_sal numeric);
 ```
 # Predict Pushdown Rules
<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-c3ow{border-color:inherit;text-align:center;vertical-align:top}
.tg .tg-dvpl{border-color:inherit;text-align:right;vertical-align:top}
</style>
<table class="tg">
<thead>
  <tr>
    <th class="tg-dvpl" rowspan="2"></th>
    <th class="tg-c3ow" colspan="2">INNER JOIN</th>
    <th class="tg-c3ow" colspan="2">LEFT JOIN</th>
    <th class="tg-c3ow" colspan="2">RIGHT_JOIN</th>
    <th class="tg-c3ow" colspan="2">FULL_JOIN</th>
  </tr>
  <tr>
    <th class="tg-c3ow">LEFT TABLE</th>
    <th class="tg-c3ow">RIGHT TABLE</th>
    <th class="tg-c3ow">LEFT TABLE</th>
    <th class="tg-c3ow">RIGHT TABLE</th>
    <th class="tg-c3ow">LEFT TABLE</th>
    <th class="tg-c3ow">RIGHT TABLE</th>
    <th class="tg-c3ow">LEFT TABLE</th>
    <th class="tg-c3ow">RIGHT TABLE</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-dvpl">JOIN_PREDICT</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Not Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Not Pushed</td>
    <td class="tg-c3ow">Not Pushed</td>
    <td class="tg-c3ow">Not Pushed</td>
  </tr>
  <tr>
    <td class="tg-dvpl">WHERE_PREDICT</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
    <td class="tg-c3ow">Pushed</td>
  </tr>
</tbody>
</table>
