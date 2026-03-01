import logging
from datetime import date, timedelta
from backend.database import get_write_connection, get_connection

logger = logging.getLogger(__name__)

HOLIDAYS_2024 = [
    ("2024-01-01", "Tahun Baru 2024 Masehi", False, False),
    ("2024-02-08", "Isra Mi'raj Nabi Muhammad SAW", False, False),
    ("2024-02-10", "Tahun Baru Imlek 2575", False, False),
    ("2024-03-11", "Hari Suci Nyepi Tahun Baru Saka 1946", False, False),
    ("2024-03-12", "Cuti Bersama Nyepi", True, False),
    ("2024-03-28", "Wafat Isa Al-Masih (Jumat Agung)", False, False),
    ("2024-03-29", "Cuti Bersama Wafat Isa Al-Masih", True, False),
    ("2024-04-08", "Cuti Bersama Idul Fitri", True, True),
    ("2024-04-09", "Cuti Bersama Idul Fitri", True, True),
    ("2024-04-10", "Hari Raya Idul Fitri 1445 H", False, True),
    ("2024-04-11", "Hari Raya Idul Fitri 1445 H", False, True),
    ("2024-04-12", "Cuti Bersama Idul Fitri", True, True),
    ("2024-04-15", "Cuti Bersama Idul Fitri", True, True),
    ("2024-05-01", "Hari Buruh Internasional", False, False),
    ("2024-05-09", "Kenaikan Isa Al-Masih", False, False),
    ("2024-05-10", "Cuti Bersama Kenaikan Isa Al-Masih", True, False),
    ("2024-05-23", "Hari Raya Waisak 2568 BE", False, False),
    ("2024-05-24", "Cuti Bersama Waisak", True, False),
    ("2024-06-01", "Hari Lahir Pancasila", False, False),
    ("2024-06-17", "Hari Raya Idul Adha 1445 H", False, False),
    ("2024-06-18", "Cuti Bersama Idul Adha", True, False),
    ("2024-07-07", "Tahun Baru Islam 1446 H", False, False),
    ("2024-08-17", "Hari Kemerdekaan RI", False, False),
    ("2024-09-16", "Maulid Nabi Muhammad SAW", False, False),
    ("2024-12-25", "Hari Raya Natal", False, False),
    ("2024-12-26", "Cuti Bersama Natal", True, False),
]

HOLIDAYS_2025 = [
    ("2025-01-01", "Tahun Baru 2025 Masehi", False, False),
    ("2025-01-27", "Isra Mi'raj Nabi Muhammad SAW", False, False),
    ("2025-01-29", "Tahun Baru Imlek 2576", False, False),
    ("2025-03-28", "Hari Suci Nyepi Tahun Baru Saka 1947", False, False),
    ("2025-03-29", "Cuti Bersama Nyepi", True, False),
    ("2025-03-31", "Hari Raya Idul Fitri 1446 H", False, True),
    ("2025-04-01", "Hari Raya Idul Fitri 1446 H", False, True),
    ("2025-04-02", "Cuti Bersama Idul Fitri", True, True),
    ("2025-04-03", "Cuti Bersama Idul Fitri", True, True),
    ("2025-04-04", "Cuti Bersama Idul Fitri", True, True),
    ("2025-04-18", "Wafat Isa Al-Masih (Jumat Agung)", False, False),
    ("2025-05-01", "Hari Buruh Internasional", False, False),
    ("2025-05-12", "Hari Raya Waisak 2569 BE", False, False),
    ("2025-05-29", "Kenaikan Isa Al-Masih", False, False),
    ("2025-06-01", "Hari Lahir Pancasila", False, False),
    ("2025-06-06", "Hari Raya Idul Adha 1446 H", False, False),
    ("2025-06-07", "Cuti Bersama Idul Adha", True, False),
    ("2025-06-27", "Tahun Baru Islam 1447 H", False, False),
    ("2025-08-17", "Hari Kemerdekaan RI", False, False),
    ("2025-09-05", "Maulid Nabi Muhammad SAW", False, False),
    ("2025-12-25", "Hari Raya Natal", False, False),
    ("2025-12-26", "Cuti Bersama Natal", True, False),
]

HOLIDAYS_2026 = [
    ("2026-01-01", "Tahun Baru 2026 Masehi", False, False),
    ("2026-01-16", "Isra Mi'raj Nabi Muhammad SAW", False, False),
    ("2026-02-17", "Tahun Baru Imlek 2577", False, False),
    ("2026-03-17", "Hari Suci Nyepi Tahun Baru Saka 1948", False, False),
    ("2026-03-20", "Hari Raya Idul Fitri 1447 H", False, True),
    ("2026-03-21", "Hari Raya Idul Fitri 1447 H", False, True),
    ("2026-04-03", "Wafat Isa Al-Masih (Jumat Agung)", False, False),
    ("2026-05-01", "Hari Buruh Internasional", False, False),
    ("2026-05-14", "Kenaikan Isa Al-Masih", False, False),
    ("2026-05-27", "Hari Raya Idul Adha 1447 H", False, False),
    ("2026-05-31", "Hari Raya Waisak 2570 BE", False, False),
    ("2026-06-01", "Hari Lahir Pancasila", False, False),
    ("2026-06-17", "Tahun Baru Islam 1448 H", False, False),
    ("2026-08-17", "Hari Kemerdekaan RI", False, False),
    ("2026-08-26", "Maulid Nabi Muhammad SAW", False, False),
    ("2026-12-25", "Hari Raya Natal", False, False),
]

HOLIDAYS_MAP = {
    2024: HOLIDAYS_2024,
    2025: HOLIDAYS_2025,
    2026: HOLIDAYS_2026,
}

RAMADAN_RANGES = {
    2024: (date(2024, 3, 12), date(2024, 4, 9)),
    2025: (date(2025, 3, 1), date(2025, 3, 30)),
    2026: (date(2026, 2, 18), date(2026, 3, 19)),
}

HOLIDAY_ICONS = {
    "Islam": "\U0001f54c",
    "Kristen": "\u26ea",
    "Hindu": "\U0001f6d5",
    "Buddha": "\U0001f6d5",
    "Nasional": "\U0001f3db\ufe0f",
}

HOLIDAY_CATEGORIES = {
    "Isra Mi'raj": "Islam",
    "Idul Fitri": "Islam",
    "Idul Adha": "Islam",
    "Tahun Baru Islam": "Islam",
    "Maulid Nabi": "Islam",
    "Ramadan": "Islam",
    "Nyepi": "Hindu",
    "Waisak": "Buddha",
    "Natal": "Kristen",
    "Wafat Isa": "Kristen",
    "Kenaikan Isa": "Kristen",
    "Jumat Agung": "Kristen",
    "Imlek": "Nasional",
}


def _get_holiday_icon(name):
    for key, cat in HOLIDAY_CATEGORIES.items():
        if key.lower() in name.lower():
            return HOLIDAY_ICONS.get(cat, "\U0001f3db\ufe0f")
    return "\U0001f3db\ufe0f"


def generate_calendar(year):
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    holidays = HOLIDAYS_MAP.get(year, [])
    holiday_map = {}
    for h_date, h_name, h_cuti, h_ramadan in holidays:
        holiday_map[h_date] = (h_name, h_cuti, h_ramadan)

    ramadan_range = RAMADAN_RANGES.get(year)
    rows = []
    current = start
    while current <= end:
        dow = current.isoweekday() % 7
        is_weekend = dow in (0, 6)
        is_holiday = False
        is_cuti = False
        is_ramadan = False
        holiday_name = None

        date_str = current.isoformat()
        if date_str in holiday_map:
            h_name, h_cuti, h_ram = holiday_map[date_str]
            holiday_name = h_name
            is_cuti = h_cuti
            is_ramadan = h_ram
            if not h_cuti:
                is_holiday = True

        if ramadan_range and ramadan_range[0] <= current <= ramadan_range[1]:
            is_ramadan = True

        if is_holiday:
            day_type = "Libur Nasional"
        elif is_cuti:
            day_type = "Cuti Bersama"
        elif is_weekend:
            day_type = "Akhir Pekan"
        else:
            day_type = "Kerja"

        rows.append((
            current, year, current.month, current.day,
            dow, current.isocalendar()[1],
            is_weekend, is_holiday, is_cuti, is_ramadan,
            holiday_name, day_type, 'built_in'
        ))
        current += timedelta(days=1)

    with get_write_connection() as conn:
        conn.execute(f"DELETE FROM ext_calendar WHERE year = {year}")
        conn.executemany(
            """INSERT INTO ext_calendar 
               (date, year, month, day, day_of_week, week_of_year,
                is_weekend, is_holiday, is_cuti_bersama, is_ramadan,
                holiday_name, day_type, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows
        )

        conn.execute("DELETE FROM ext_annotations WHERE source = 'built_in' AND date >= ? AND date <= ?",
                      [start, end])
        ann_id_base = year * 1000
        ann_rows = []
        for i, (h_date, h_name, h_cuti, h_ram) in enumerate(holidays):
            if h_cuti:
                continue
            icon = _get_holiday_icon(h_name)
            ann_rows.append((
                ann_id_base + i, h_date, None, None, None, None,
                'holiday', h_name, f"Hari libur nasional: {h_name}",
                'info', '#3B82F6', icon, True, 'built_in'
            ))

        if ramadan_range:
            ann_rows.append((
                ann_id_base + 900, ramadan_range[0].isoformat(), ramadan_range[1].isoformat(),
                None, None, None,
                'holiday', f'Bulan Ramadan {year}', f'Periode Ramadan {ramadan_range[0]} - {ramadan_range[1]}',
                'info', '#10B981', '\U0001f54c', True, 'built_in'
            ))

        for row in ann_rows:
            try:
                conn.execute(
                    """INSERT INTO ext_annotations
                       (id, date, date_end, area_id, regional_id, province,
                        annotation_type, title, description, severity, color, icon,
                        show_on_chart, source)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT DO NOTHING""",
                    list(row)
                )
            except Exception:
                pass

    holidays_count = sum(1 for _, _, c, _ in holidays if not c)
    cuti_count = sum(1 for _, _, c, _ in holidays if c)
    return {
        "year": year,
        "dates": len(rows),
        "holidays": holidays_count,
        "cuti_bersama": cuti_count,
        "annotations_created": len(ann_rows),
    }


def seed_calendar_if_empty():
    with get_connection() as conn:
        try:
            count = conn.execute("SELECT COUNT(*) FROM ext_calendar").fetchone()[0]
            if count > 0:
                has_2024 = conn.execute("SELECT COUNT(*) FROM ext_calendar WHERE year = 2024").fetchone()[0]
                if has_2024 == 0:
                    logger.info("Adding calendar 2024...")
                    generate_calendar(2024)
                    logger.info("Calendar 2024 added")
                return
        except Exception:
            return

    logger.info("Seeding calendar for 2024-2026...")
    generate_calendar(2024)
    generate_calendar(2025)
    generate_calendar(2026)
    logger.info("Calendar seeded: 2024 + 2025 + 2026")
