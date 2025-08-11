-- json diff util
      with recursive 
        _j as (select array[k1] as kp,array[1] as kf,1 as le,jsonb_typeof(y #> array[k1]) as ty,y #> array[k1] as jb
                 from (values ('{}'::jsonb)) as _(y) 
           cross join lateral jsonb_object_keys(y) k1 
                where y #> array[k1] is not null
                union all
               select kp || case ty when 'object' then ko else ka end as kp,kf || case ty when 'object' then 1 else 0 end as kf,le + 1 as le,jsonb_typeof(jb #> array[case ty when 'object' then ko else ka end]) as ty,jb #> array[case ty when 'object' then ko else ka end] as jb
                 from _j
           cross join lateral (select jsonb_object_keys(case ty when 'object' then jb else '{}'::jsonb end) as ko,cast(generate_series(0, jsonb_array_length(case ty when 'array' then jb else '[]'::jsonb end) - 1) as text) as ka) k2 
                where ty in ('object','array')),
        j_ as (select array[k1] as kp,array[1] as kf,1 as le,jsonb_typeof(y #> array[k1]) as ty,y #> array[k1] as jb
                 from (values ('{}'::jsonb)) as _(y) 
           cross join lateral jsonb_object_keys(y) k1 
                where y #> array[k1] is not null
                union all
               select kp || case ty when 'object' then ko else ka end as kp,kf || case ty when 'object' then 1 else 0 end as kf,le + 1 as le,jsonb_typeof(jb #> array[case ty when 'object' then ko else ka end]) as ty,jb #> array[case ty when 'object' then ko else ka end] as jb
                 from j_
           cross join lateral (select jsonb_object_keys(case ty when 'object' then jb else '{}'::jsonb end) as ko,cast(generate_series(0, jsonb_array_length(case ty when 'array' then jb else '[]'::jsonb end) - 1) as text) as ka) k2 
                where ty in ('object','array')),
        d_ as (select _j.kp as _kp,_j.kf as _kf,_j.le as _le,_j.ty as _ty,_j.jb as _jb,j_.kp as kp_,j_.kf as kf_,j_.le as le_,j_.ty as ty_,j_.jb as jb_
                     ,coalesce(_j.le,j_.le) as le,coalesce(_j.kp,j_.kp) as kp,case when _j.kp is null then '+' when j_.kp is null then '-' when _j.jb != j_.jb then 'U' else null end as df
                 from _j 
            full join  j_ 
                   on _j .kp = j_.kp
                  and _j .le = j_.le
                  and _j .ty = j_.ty
                  and _j .kf = j_.kf
                where (case when _j.kp is null then '+' when j_.kp is null then '-' when _j.jb != j_.jb then 'U' else null end) is not null),
        e_ as (select _kp,_kf,_le,_ty,_jb,kp_,kf_,le_,ty_,jb_,df,le,kp
                 from d_
                where le = 1
                union all
               select d_._kp,d_._kf,d_._le,d_._ty,d_._jb,d_.kp_,d_.kf_,d_.le_,d_.ty_,d_.jb_,d_.df,d_.le,d_.kp
                 from d_
           inner join e_
                   on d_.le = e_.le + 1
                  and d_.kp[1 : e_.le] = e_.kp
                  and e_.df = 'U')
    select df,le,kp,_ty,_jb,ty_,jb_ from e_ where (df = 'U' and ty_ not in ('object','array')) or (df in ('-','+')) order by le asc,kp asc,df desc;

-- lock monitor
  with t_wait as
      (select a.mode,a.locktype,a.database,a.relation,a.page,a.tuple,a.classid,a.granted,a.objid,a.objsubid,a.pid,a.virtualtransaction,a.virtualxid,a.transactionid,a.fastpath,b.state,b.query,b.xact_start,b.query_start,b.usename,b.datname,b.client_addr,b.client_port,b.application_name   
         from pg_locks a,pg_stat_activity b 
        where a.pid=b.pid 
          and not a.granted)
      ,t_run as
      (select a.mode,a.locktype,a.database,a.relation,a.page,a.tuple,a.classid,a.granted,a.objid,a.objsubid,a.pid,a.virtualtransaction,a.virtualxid,a.transactionid,a.fastpath,b.state,b.query,b.xact_start,b.query_start,b.usename,b.datname,b.client_addr,b.client_port,b.application_name   
         from pg_locks a,pg_stat_activity b 
        where a.pid=b.pid 
          and a.granted)
      ,t_overlap as
      (select r.*
         from t_wait w
         join t_run r
           on r.locktype         is not distinct from w.locktype
          and r.database         is not distinct from w.database
          and r.relation         is not distinct from w.relation
          and r.page             is not distinct from w.page
          and r.tuple            is not distinct from w.tuple
          and r.virtualxid       is not distinct from w.virtualxid
          and r.transactionid    is not distinct from w.transactionid
          and r.classid          is not distinct from w.classid
          and r.objid            is not distinct from w.objid
          and r.objsubid         is not distinct from w.objsubid
          and r.pid <> w.pid)
      ,t_unionall as
      (select r.* from t_overlap r
        union all
       select w.* from t_wait w)
select locktype,datname,relation::regclass,page,tuple,virtualxid,transactionid::text,classid::regclass,objid,objsubid
      ,string_agg('Pid: '|| case when pid is null then 'NULL' else pid::text end || chr(10) || 'Lock_Granted: ' || case when granted is null then 'NULL' else granted::text end ||' , Mode: ' || case when mode is null then 'NULL' else mode::text end || ' , FastPath: ' || case when fastpath is null then 'NULL' else fastpath::text end || ' , VirtualTransaction: ' || case when virtualtransaction is null then 'NULL' else virtualtransaction::text end || ' , Session_State: ' || case when state is null then 'NULL' else state::text end || chr(10) || 'Username: ' || case when usename is null then 'NULL' else usename::text end || ' , Database: ' || case when datname is null then 'NULL' else datname::text end || ' , Client_Addr: ' || case when client_addr is null then 'NULL' else client_addr::text end || ' , Client_Port: ' || case when client_port is null then 'NULL' else client_port::text end || ' , Application_Name: ' || case when application_name is null then 'NULL' else application_name::text end || chr(10) || 'Xact_Start: ' || case when xact_start is null then 'NULL' else xact_start::text end || ' , Query_Start: ' || case when query_start is null then 'NULL' else query_start::text end || ' , Xact_Elapse: ' || case when (now() - xact_start) is null then 'NULL' else (now() - xact_start)::text end || ' , Query_Elapse: ' || case when (now() - query_start) is null then 'NULL' else (now() - query_start)::text end || chr(10) || 'SQL (Current SQL in Transaction): ' || chr(10) || case when query is null then 'NULL' else query::text end, chr(10) || '--------' || chr(10) order by (case mode when 'INVALID' then 0 when 'AccessShareLock' then 1 when 'RowShareLock' then 2 when 'RowExclusiveLock' then 3 when 'ShareUpdateExclusiveLock' then 4 when 'ShareLock' then 5 when 'ShareRowExclusiveLock' then 6 when 'ExclusiveLock' then 7 when 'AccessExclusiveLock' then 8 else 0 end) desc,(case when granted then 0 else 1 end)) as lock_conflict  
  from t_unionall   
 group by locktype,datname,relation,page,tuple,virtualxid,transactionid::text,classid,objid,objsubid;
