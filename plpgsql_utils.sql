-- json diff util
create or replace function jsonb_diff(jsonb, jsonb, text[] default '{}'::text[], integer default 0) returns jsonb as 
$$
declare
    _t  text := jsonb_typeof($1);
     t_ text := jsonb_typeof($2);
    _k  text[];
     k_ text[];
     k  text;
    _l  integer;
     l_ integer;
     i  integer;
     r  jsonb := '[]'::jsonb;
begin
    if _t <> t_ then
        if _t is not null then
        r := r || jsonb_build_object('e','-','l',$4,'_k',$3,'k_',null,'_v',$1,'v_',null);
        end if;
        if t_ is not null then
        r := r || jsonb_build_object('e','+','l',$4,'_k',null,'k_',$3,'_v',null,'v_',$2);
        end if;
    else
        if t_ = 'object' then
            _k  := array(select jsonb_object_keys($1));
             k_ := array(select jsonb_object_keys($2));
            foreach k in array array(select unnest(_k) except select unnest(k_))
            loop
                r := r || jsonb_build_object('e','-','l',$4 + 1,'_k',$3 || k,'k_',null,'_v',$1 #> array[k],'v_',null);
            end loop;
            foreach k in array array(select unnest(k_) except select unnest(_k))
            loop
                r := r || jsonb_build_object('e','+','l',$4 + 1,'_k',null,'k_',$3 || k,'_v',null,'v_',$2 #> array[k]);
            end loop;
            foreach k in array array(select unnest(_k) intersect select unnest(k_))
            loop
                r := r || jsonb_diff($1 #> array[k], $2 #> array[k], $3 || k, $4 + 1);
            end loop;
        elsif t_ = 'array' then
            _l  := jsonb_array_length($1);
             l_ := jsonb_array_length($2);
            for i in 0 .. least(_l,l_) - 1
            loop
                r := r || jsonb_diff($1 #> array[i::text], $2 #> array[i::text], $3 || i::text, $4 + 1);
            end loop;
            if _l > l_ then
                for i in l_ .. _l - 1
                loop
                    r := r || jsonb_build_object('e','-','l',$4 + 1,'_k',$3 || i::text,'k_',null,'_v',$1 #> array[i::text],'v_',null);
                end loop;
            elsif  _l < l_ then
                for i in _l .. l_ - 1
                loop
                    r := r || jsonb_build_object('e','+','l',$4 + 1,'_k',null,'k_',$3 || i::text,'_v',null,'v_',$2 #> array[i::text]);
                end loop;
            end if;
        else
            if $1 <> $2 then
                r := r || jsonb_build_object('e','u','l',$4,'_k',$3,'k_',$3,'_v',$1,'v_',$2);
            end if;
        end if;
    end if;
    return r;
end;
$$ language plpgsql;
