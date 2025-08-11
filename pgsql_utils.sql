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
