#!/usr/bin/env python3
import argparse
import csv
import sqlite3
import sys
from pathlib import Path


RU = {
    "ALEXANDR": "\u0410\u043b\u0435\u043a\u0441\u0430\u043d\u0434\u0440",
    "DEPT_CT": (
        "\u041a\u0430\u0444\u0435\u0434\u0440\u0430 "
        "\u0432\u044b\u0447\u0438\u0441\u043b\u0438\u0442\u0435\u043b\u044c\u043d\u043e\u0439 "
        "\u0442\u0435\u0445\u043d\u0438\u043a\u0438"
    ),
    "FKTIU": "\u0424\u041a\u0422\u0418\u0423",
    "FULL_TIME": "\u043e\u0447\u043d\u0430\u044f",
    "PART_TIME": "\u0437\u0430\u043e\u0447\u043d\u0430\u044f",
    "VLADIMIROVICH": "\u0412\u043b\u0430\u0434\u0438\u043c\u0438\u0440\u043e\u0432\u0438\u0447",
}


QUERY_CASES = {
    "48.1": {
        "sql": """
select coalesce(cast(t.id as text), 'NULL') || ',' || v.id
from vedomosti v
left join types_vedomostei t on t.id = v.type_id and t.id > 2
where v.person_id > 153285
order by 1;
""",
        "expected": ["3,2001", "3,2007", "4,2002", "5,2004", "NULL,2005"],
    },
    "48.2": {
        "sql": """
select p.surname || ',' || p.id || ',' || st.group_name
from people p
join studies s on s.person_id = p.id and s.person_id < 112514
join students st on st.person_id = p.id and st.start_date <> '' and st.start_date < '2008-09-01'
where p.patronymic < '{VLADIMIROVICH}'
order by 1;
""",
        "expected": ["Демин,100020,2200"],
    },
    "48.3": {
        "sql": """
select count(distinct birthday)
from people;
""",
        "expected": ["20"],
    },
    "48.4": {
        "sql": """
select p.patronymic || ',' || count(*)
from studies s
join people p on p.id = s.person_id
where s.form = '{PART_TIME}'
group by p.patronymic
having count(*) = 10
order by 1;
""",
        "expected": ["Сергеевич,10"],
    },
    "48.5": {
        "sql": """
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
""",
        "expected": ["2100,19.00"],
    },
    "48.6": {
        "sql": """
select st.group_name || ',' || p.id || ',' || p.surname || ',' || p.name || ',' || p.patronymic ||
       ',' || st.order_number || ',' || st.order_state
from students st
join studies s on s.person_id = st.person_id
join people p on p.id = st.person_id
where s.direction_code = '230101'
  and s.form = '{FULL_TIME}'
  and s.course = 1
  and st.start_date < '2012-09-01'
order by 1;
""",
        "expected": [
            "1100,153285,Лисин,Павел,Олегович,PR-285,Зачислен",
            "1100,163249,Матвеев,Руслан,Петрович,PR-249,Зачислен",
            "2200,100020,Демин,Артем,Александрович,PR-020,Зачислен",
        ],
    },
    "48.7": {
        "sql": """
select count(*)
from studies s
where s.faculty = '{FKTIU}'
  and exists (select 1 from vedomosti v where v.person_id = s.person_id)
  and not exists (select 1 from vedomosti v where v.person_id = s.person_id and v.mark <> 5);
""",
        "expected": ["3"],
    },
    "78.1": {
        "sql": """
select coalesce(cast(t.id as text), 'NULL') || ',' || v.date
from vedomosti v
left join types_vedomostei t on t.id = v.type_id and t.id < 2
where v.person_id = 153285 and v.person_id = 163249
order by 1;
""",
        "expected": [],
    },
    "78.2": {
        "sql": """
select p.surname || ',' || s.nzk || ',' || st.start_date
from people p
join studies s on s.person_id = p.id and s.person_id = 113409
join students st on st.person_id = p.id
where p.name > '{ALEXANDR}'
order by 1;
""",
        "expected": ["Зуев,OK409,2011-09-01"],
    },
    "78.3": {
        "sql": """
select count(distinct birthday)
from people;
""",
        "expected": ["20"],
    },
    "78.4": {
        "sql": """
select st.group_name || ',' || count(*)
from students st
join studies s on s.person_id = st.person_id
where s.department = '{DEPT_CT}'
  and st.start_date <= '2011-12-31'
  and (st.end_date = '' or st.end_date >= '2011-01-01')
group by st.group_name
having count(*) < 5
order by 1;
""",
        "expected": ["1100,2", "2100,1", "2200,1", "4100,3"],
    },
    "78.5": {
        "sql": """
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
""",
        "expected": ["2100,19.00", "2200,18.00", "3100,20.00", "4100,21.67", "5100,20.00"],
    },
    "78.6": {
        "sql": """
select st.group_name || ',' || p.id || ',' || p.surname || ',' || p.name || ',' || p.patronymic ||
       ',' || st.order_number
from students st
join studies s on s.person_id = st.person_id
join people p on p.id = st.person_id
where s.direction_code = '230101'
  and s.form in ('{FULL_TIME}', '{PART_TIME}')
  and st.status = 'expelled'
  and st.end_date > '2012-09-01'
order by 1;
""",
        "expected": ["2100,170000,Новиков,Степан,Андреевич,PR-700"],
    },
    "78.7": {
        "sql": """
select p.id || ',' || p.surname || ',' || p.name || ',' || p.patronymic
from people p
left join students st on st.person_id = p.id
where st.person_id is null
order by 1;
""",
        "expected": [
            "180000,Осипова,Татьяна,Викторовна",
            "190000,Романов,Юрий,Владимирович",
        ],
    },
}


DDL = """
create table types_vedomostei(id integer, name text);
create table vedomosti(id integer, type_id integer, person_id integer, date text, mark integer);
create table people(id integer, surname text, name text, patronymic text, birthday text);
create table studies(
    person_id integer,
    nzk text,
    form text,
    direction_code text,
    department text,
    faculty text,
    course integer
);
create table students(
    person_id integer,
    group_name text,
    start_date text,
    end_date text,
    order_number text,
    order_state text,
    status text
);
"""


def load_table(cursor: sqlite3.Cursor, csv_path: Path, table_name: str) -> None:
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        placeholders = ",".join("?" for _ in header)
        cursor.executemany(
            f"insert into {table_name} values ({placeholders})",
            list(reader),
        )


def build_connection(data_dir: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.executescript(DDL)
    for table_name in (
        "types_vedomostei",
        "vedomosti",
        "people",
        "studies",
        "students",
    ):
        load_table(cursor, data_dir / f"{table_name}.csv", table_name)
    conn.commit()
    return conn


def rows_to_strings(cursor: sqlite3.Cursor, sql: str) -> list[str]:
    result = []
    for row in cursor.execute(sql):
        result.append(",".join("" if value is None else str(value) for value in row))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify that SQL queries over CSV-imported data match the SPO task2 outputs."
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Path to the directory with CSV files (default: data).",
    )
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    data_dir = Path(args.data_dir).resolve()
    conn = build_connection(data_dir)
    cursor = conn.cursor()

    ok = True
    for label, case in QUERY_CASES.items():
        sql = case["sql"].format(**RU)
        actual = rows_to_strings(cursor, sql)
        expected = case["expected"]
        if actual != expected:
            ok = False
            print(f"FAIL {label}")
            print("  expected:")
            if expected:
                for row in expected:
                    print(f"    {row}")
            else:
                print("    (no rows)")
            print("  actual:")
            if actual:
                for row in actual:
                    print(f"    {row}")
            else:
                print("    (no rows)")
            continue

        print(f"PASS {label}")
        if actual:
            for row in actual:
                print(f"  {row}")
        else:
            print("  (no rows)")

    conn.close()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
