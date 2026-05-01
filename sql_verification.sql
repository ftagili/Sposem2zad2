-- 48.1
select coalesce(cast(t.id as text), 'NULL') || ',' || v.id
from vedomosti v
left join types_vedomostei t on t.id = v.type_id and t.id > 2
where v.person_id > 153285
order by 1;

-- 48.2
select p.surname || ',' || p.id || ',' || st.group_name
from people p
join studies s on s.person_id = p.id and s.person_id < 112514
join students st on st.person_id = p.id and st.start_date <> '' and st.start_date < '2008-09-01'
where p.patronymic < 'Владимирович'
order by 1;

-- 48.3
select count(distinct birthday)
from people;

-- 48.4
select p.patronymic || ',' || count(*)
from studies s
join people p on p.id = s.person_id
where s.form = 'заочная'
group by p.patronymic
having count(*) = 10
order by 1;

-- 48.5
with groups as (
    select st.group_name,
           avg(
               (cast(strftime('%Y', '2025-01-01') as integer) - cast(strftime('%Y', p.birthday) as integer)) -
               (strftime('%m-%d', '2025-01-01') < strftime('%m-%d', p.birthday))
           ) as avg_age
    from students st
    join people p on p.id = st.person_id
    group by st.group_name
),
reference_age as (
    select min(
               (cast(strftime('%Y', '2025-01-01') as integer) - cast(strftime('%Y', p.birthday) as integer)) -
               (strftime('%m-%d', '2025-01-01') < strftime('%m-%d', p.birthday))
           ) as age_value
    from students st
    join people p on p.id = st.person_id
    where st.group_name = '3100'
)
select groups.group_name || ',' || printf('%.2f', groups.avg_age)
from groups, reference_age
where abs(groups.avg_age - reference_age.age_value) < 0.0001
order by 1;

-- 48.6
select st.group_name || ',' || p.id || ',' || p.surname || ',' || p.name || ',' || p.patronymic ||
       ',' || st.order_number || ',' || st.order_state
from students st
join studies s on s.person_id = st.person_id
join people p on p.id = st.person_id
where s.direction_code = '230101'
  and s.form = 'очная'
  and s.course = 1
  and st.start_date < '2012-09-01'
order by 1;

-- 48.7
select count(*)
from studies s
where s.faculty = 'ФКТИУ'
  and exists (select 1 from vedomosti v where v.person_id = s.person_id)
  and not exists (select 1 from vedomosti v where v.person_id = s.person_id and v.mark <> 5);

-- 78.1
select coalesce(cast(t.id as text), 'NULL') || ',' || v.date
from vedomosti v
left join types_vedomostei t on t.id = v.type_id and t.id < 2
where v.person_id = 153285 and v.person_id = 163249
order by 1;

-- 78.2
select p.surname || ',' || s.nzk || ',' || st.start_date
from people p
join studies s on s.person_id = p.id and s.person_id = 113409
join students st on st.person_id = p.id
where p.name > 'Александр'
order by 1;

-- 78.3
select count(distinct birthday)
from people;

-- 78.4
select st.group_name || ',' || count(*)
from students st
join studies s on s.person_id = st.person_id
where s.department = 'Кафедра вычислительной техники'
  and st.start_date <= '2011-12-31'
  and (st.end_date = '' or st.end_date >= '2011-01-01')
group by st.group_name
having count(*) < 5
order by 1;

-- 78.5
with groups as (
    select st.group_name,
           avg(
               (cast(strftime('%Y', '2025-01-01') as integer) - cast(strftime('%Y', p.birthday) as integer)) -
               (strftime('%m-%d', '2025-01-01') < strftime('%m-%d', p.birthday))
           ) as avg_age
    from students st
    join people p on p.id = st.person_id
    group by st.group_name
),
reference_age as (
    select min(
               (cast(strftime('%Y', '2025-01-01') as integer) - cast(strftime('%Y', p.birthday) as integer)) -
               (strftime('%m-%d', '2025-01-01') < strftime('%m-%d', p.birthday))
           ) as age_value
    from students st
    join people p on p.id = st.person_id
    where st.group_name = '1100'
)
select groups.group_name || ',' || printf('%.2f', groups.avg_age)
from groups, reference_age
where groups.avg_age < reference_age.age_value
order by 1;

-- 78.6
select st.group_name || ',' || p.id || ',' || p.surname || ',' || p.name || ',' || p.patronymic ||
       ',' || st.order_number
from students st
join studies s on s.person_id = st.person_id
join people p on p.id = st.person_id
where s.direction_code = '230101'
  and s.form in ('очная', 'заочная')
  and st.status = 'expelled'
  and st.end_date > '2012-09-01'
order by 1;

-- 78.7
select p.id || ',' || p.surname || ',' || p.name || ',' || p.patronymic
from people p
left join students st on st.person_id = p.id
where st.person_id is null
order by 1;
