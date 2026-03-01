import logging
from backend.database import get_write_connection, get_connection

logger = logging.getLogger(__name__)

TABLES_DDL = [
    ("master_area", """
        CREATE TABLE IF NOT EXISTS master_area (
            area_id VARCHAR PRIMARY KEY,
            area_name VARCHAR NOT NULL,
            area_alias VARCHAR,
            description VARCHAR,
            status VARCHAR DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("master_regional", """
        CREATE TABLE IF NOT EXISTS master_regional (
            regional_id VARCHAR PRIMARY KEY,
            regional_name VARCHAR NOT NULL,
            area_id VARCHAR NOT NULL,
            regional_alias_site_master VARCHAR,
            regional_alias_ticket VARCHAR,
            description VARCHAR,
            status VARCHAR DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("master_nop", """
        CREATE TABLE IF NOT EXISTS master_nop (
            nop_id VARCHAR PRIMARY KEY,
            nop_name VARCHAR NOT NULL,
            regional_id VARCHAR NOT NULL,
            nop_alias_site_master VARCHAR,
            nop_alias_ticket VARCHAR,
            description VARCHAR,
            status VARCHAR DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("master_to", """
        CREATE TABLE IF NOT EXISTS master_to (
            to_id VARCHAR PRIMARY KEY,
            to_name VARCHAR NOT NULL,
            nop_id VARCHAR NOT NULL,
            to_alias_site_master VARCHAR,
            to_alias_ticket VARCHAR,
            description VARCHAR,
            status VARCHAR DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("master_site", """
        CREATE TABLE IF NOT EXISTS master_site (
            site_id VARCHAR PRIMARY KEY,
            site_name VARCHAR NOT NULL,
            to_id VARCHAR,
            site_class VARCHAR NOT NULL,
            site_flag VARCHAR NOT NULL,
            site_category VARCHAR,
            site_sub_class VARCHAR,
            upgrade_potential VARCHAR,
            est_technology VARCHAR,
            est_transmission VARCHAR,
            est_power VARCHAR,
            est_sector VARCHAR,
            complexity_level INTEGER,
            est_opex_level VARCHAR,
            strategy_focus VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            province VARCHAR,
            city VARCHAR,
            district VARCHAR,
            address VARCHAR,
            timezone VARCHAR DEFAULT 'Asia/Jakarta',
            primary_equipment_type VARCHAR,
            equipment_count INTEGER,
            equipment_age_years DOUBLE,
            commissioning_date DATE,
            status VARCHAR DEFAULT 'ACTIVE',
            source VARCHAR DEFAULT 'site_master',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("master_sla_target", """
        CREATE TABLE IF NOT EXISTS master_sla_target (
            id INTEGER PRIMARY KEY,
            area_id VARCHAR DEFAULT '*',
            regional_id VARCHAR DEFAULT '*',
            site_class VARCHAR DEFAULT '*',
            site_flag VARCHAR DEFAULT '*',
            severity VARCHAR DEFAULT '*',
            sla_target_pct DOUBLE NOT NULL,
            mttr_target_min DOUBLE,
            response_target_min DOUBLE,
            priority INTEGER DEFAULT 0,
            description VARCHAR,
            effective_from DATE,
            effective_to DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("master_threshold", """
        CREATE TABLE IF NOT EXISTS master_threshold (
            param_key VARCHAR PRIMARY KEY,
            param_value DOUBLE NOT NULL,
            param_unit VARCHAR,
            category VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("orphan_log", """
        CREATE TABLE IF NOT EXISTS orphan_log (
            id INTEGER PRIMARY KEY,
            source VARCHAR NOT NULL,
            level VARCHAR NOT NULL,
            value VARCHAR NOT NULL,
            suggested_match VARCHAR,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_to VARCHAR,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP,
            ticket_count INTEGER DEFAULT 1
        )
    """),
    ("noc_tickets", """
        CREATE TABLE IF NOT EXISTS noc_tickets (
            ticket_number_inap VARCHAR,
            ticket_number_swfm VARCHAR,
            ticket_creation VARCHAR,
            ticket_creator VARCHAR,
            severity VARCHAR,
            type_ticket VARCHAR,
            fault_level VARCHAR,
            impact VARCHAR,
            ne_class VARCHAR,
            incident_priority VARCHAR,
            site_id VARCHAR,
            site_name VARCHAR,
            site_class VARCHAR,
            cluster_to VARCHAR,
            sub_cluster VARCHAR,
            nop VARCHAR,
            regional VARCHAR,
            area VARCHAR,
            hub VARCHAR,
            occured_time TIMESTAMP,
            created_at TIMESTAMP,
            cleared_time TIMESTAMP,
            submitted_time TIMESTAMP,
            take_over_date TIMESTAMP,
            check_in_at TIMESTAMP,
            dispatch_date TIMESTAMP,
            follow_up_at TIMESTAMP,
            closed_at TIMESTAMP,
            site_cleared_on TIMESTAMP,
            rca_validate_at TIMESTAMP,
            duration_ticket VARCHAR,
            age_ticket VARCHAR,
            rh_start VARCHAR,
            rh_start_time VARCHAR,
            rh_stop VARCHAR,
            rh_stop_time VARCHAR,
            ticket_inap_status VARCHAR,
            ticket_swfm_status VARCHAR,
            sla_status VARCHAR,
            holding_status VARCHAR,
            pic_take_over_ticket VARCHAR,
            is_escalate VARCHAR,
            escalate_to VARCHAR,
            is_auto_resolved VARCHAR,
            assignee_group VARCHAR,
            dispatch_by VARCHAR,
            is_force_dispatch VARCHAR,
            is_excluded_in_kpi VARCHAR,
            rc_owner VARCHAR,
            rc_category VARCHAR,
            rc_1 VARCHAR,
            rc_2 VARCHAR,
            inap_rc_1 VARCHAR,
            inap_rc_2 VARCHAR,
            resolution_action VARCHAR,
            inap_resolution_action VARCHAR,
            rc_owner_engineer VARCHAR,
            rc_category_engineer VARCHAR,
            rc_1_engineer VARCHAR,
            rc_2_engineer VARCHAR,
            rca_validated VARCHAR,
            rca_validated_by VARCHAR,
            summary VARCHAR,
            description VARCHAR,
            note VARCHAR,
            nossa_no VARCHAR,
            rank VARCHAR,
            pic_email VARCHAR,
            rat VARCHAR,
            parking_status VARCHAR,
            parking_start VARCHAR,
            parking_end VARCHAR,
            yearmonth VARCHAR,
            calc_response_time_min DOUBLE,
            calc_repair_time_min DOUBLE,
            calc_restore_time_min DOUBLE,
            calc_detection_time_min DOUBLE,
            calc_sla_duration_min DOUBLE,
            calc_sla_target_min DOUBLE,
            calc_is_sla_met BOOLEAN,
            calc_hour_of_day INTEGER,
            calc_day_of_week INTEGER,
            calc_week_of_month INTEGER,
            calc_month INTEGER,
            calc_year INTEGER,
            calc_year_month VARCHAR,
            calc_year_week VARCHAR,
            calc_area_id VARCHAR,
            calc_regional_id VARCHAR,
            calc_nop_id VARCHAR,
            calc_to_id VARCHAR,
            calc_source VARCHAR
        )
    """),
    ("summary_monthly", """
        CREATE TABLE IF NOT EXISTS summary_monthly (
            year_month VARCHAR,
            area_id VARCHAR,
            regional_id VARCHAR,
            nop_id VARCHAR,
            to_id VARCHAR,
            site_id VARCHAR,
            severity VARCHAR,
            type_ticket VARCHAR,
            fault_level VARCHAR,
            total_tickets INTEGER,
            total_sla_met INTEGER,
            sla_pct DOUBLE,
            avg_mttr_min DOUBLE,
            avg_response_min DOUBLE,
            total_escalated INTEGER,
            escalation_pct DOUBLE,
            total_auto_resolved INTEGER,
            auto_resolve_pct DOUBLE,
            total_repeat INTEGER,
            repeat_pct DOUBLE,
            count_critical INTEGER,
            count_major INTEGER,
            count_minor INTEGER,
            count_low INTEGER,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("summary_weekly", """
        CREATE TABLE IF NOT EXISTS summary_weekly (
            year_week VARCHAR,
            area_id VARCHAR,
            regional_id VARCHAR,
            nop_id VARCHAR,
            to_id VARCHAR,
            site_id VARCHAR,
            total_tickets INTEGER,
            sla_pct DOUBLE,
            avg_mttr_min DOUBLE,
            avg_response_min DOUBLE,
            total_escalated INTEGER,
            escalation_pct DOUBLE,
            total_auto_resolved INTEGER,
            auto_resolve_pct DOUBLE,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("risk_score_history", """
        CREATE TABLE IF NOT EXISTS risk_score_history (
            calculation_date DATE,
            site_id VARCHAR,
            frequency_score DOUBLE,
            recency_score DOUBLE,
            severity_score DOUBLE,
            repeat_score DOUBLE,
            mttr_trend_score DOUBLE,
            escalation_score DOUBLE,
            device_risk_score DOUBLE,
            total_risk_score DOUBLE,
            risk_level VARCHAR,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (calculation_date, site_id)
        )
    """),
    ("saved_views", """
        CREATE TABLE IF NOT EXISTS saved_views (
            id INTEGER PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            entity_level VARCHAR(20) NOT NULL,
            entity_id VARCHAR(50) NOT NULL,
            entity_name VARCHAR(200),
            granularity VARCHAR(20) NOT NULL DEFAULT 'monthly',
            date_from VARCHAR(10),
            date_to VARCHAR(10),
            type_ticket VARCHAR(20),
            severities TEXT,
            fault_level VARCHAR(100),
            rc_category VARCHAR(100),
            snapshot_sla REAL,
            snapshot_mttr REAL,
            snapshot_volume INTEGER,
            snapshot_escalation REAL,
            snapshot_auto_resolve REAL,
            snapshot_repeat REAL,
            snapshot_behavior VARCHAR(30),
            snapshot_status VARCHAR(30),
            snapshot_risk_score REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_accessed_at TIMESTAMP,
            access_count INTEGER DEFAULT 0,
            is_pinned BOOLEAN DEFAULT FALSE,
            sort_order INTEGER DEFAULT 0,
            url_params TEXT
        )
    """),
    ("report_history", """
        CREATE TABLE IF NOT EXISTS report_history (
            id INTEGER PRIMARY KEY,
            report_type VARCHAR(20) NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            period_label VARCHAR(100),
            entity_level VARCHAR(20) NOT NULL,
            entity_id VARCHAR(50) NOT NULL,
            entity_name VARCHAR(200),
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            generated_by VARCHAR(100) DEFAULT 'manual',
            generation_time_ms INTEGER,
            ai_enhanced BOOLEAN DEFAULT FALSE,
            pdf_path VARCHAR(500),
            pdf_size_kb INTEGER,
            excel_path VARCHAR(500),
            excel_size_kb INTEGER,
            status VARCHAR(20) DEFAULT 'pending',
            error_message TEXT,
            metadata TEXT
        )
    """),
    ("import_logs", """
        CREATE TABLE IF NOT EXISTS import_logs (
            id INTEGER PRIMARY KEY,
            filename VARCHAR NOT NULL,
            file_type VARCHAR NOT NULL,
            file_size_mb DOUBLE,
            period VARCHAR,
            rows_total INTEGER,
            rows_imported INTEGER,
            rows_skipped INTEGER,
            rows_error INTEGER,
            orphan_count INTEGER,
            processing_time_sec DOUBLE,
            status VARCHAR DEFAULT 'completed',
            error_message VARCHAR,
            backup_created BOOLEAN DEFAULT FALSE,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ext_weather", """
        CREATE TABLE IF NOT EXISTS ext_weather (
            id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            province VARCHAR NOT NULL,
            city VARCHAR,
            rainfall_mm DOUBLE,
            temperature_avg_c DOUBLE,
            temperature_max_c DOUBLE,
            temperature_min_c DOUBLE,
            humidity_avg_pct DOUBLE,
            wind_speed_avg_kmh DOUBLE,
            weather_condition VARCHAR,
            is_extreme BOOLEAN DEFAULT FALSE,
            source VARCHAR DEFAULT 'BMKG',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ext_pln_outage", """
        CREATE TABLE IF NOT EXISTS ext_pln_outage (
            id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            province VARCHAR NOT NULL,
            city VARCHAR,
            district VARCHAR,
            outage_type VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_hours DOUBLE,
            affected_area VARCHAR,
            source VARCHAR DEFAULT 'PLN',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ext_calendar", """
        CREATE TABLE IF NOT EXISTS ext_calendar (
            date DATE PRIMARY KEY,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            week_of_year INTEGER,
            is_weekend BOOLEAN DEFAULT FALSE,
            is_holiday BOOLEAN DEFAULT FALSE,
            is_cuti_bersama BOOLEAN DEFAULT FALSE,
            is_ramadan BOOLEAN DEFAULT FALSE,
            holiday_name VARCHAR,
            day_type VARCHAR NOT NULL,
            source VARCHAR DEFAULT 'built_in',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ext_annotations", """
        CREATE TABLE IF NOT EXISTS ext_annotations (
            id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            date_end DATE,
            area_id VARCHAR,
            regional_id VARCHAR,
            province VARCHAR,
            annotation_type VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            description VARCHAR,
            severity VARCHAR DEFAULT 'info',
            color VARCHAR,
            icon VARCHAR,
            show_on_chart BOOLEAN DEFAULT TRUE,
            source VARCHAR DEFAULT 'manual',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("site_risk_scores", """
        CREATE TABLE IF NOT EXISTS site_risk_scores (
            site_id VARCHAR NOT NULL,
            calculated_at TIMESTAMP NOT NULL,
            risk_score DOUBLE NOT NULL,
            frequency_score DOUBLE,
            recency_score DOUBLE,
            severity_score DOUBLE,
            mttr_trend_score DOUBLE,
            repeat_score DOUBLE,
            device_score DOUBLE,
            escalation_score DOUBLE,
            risk_level VARCHAR,
            top_component VARCHAR,
            predicted_next VARCHAR,
            pattern_detected BOOLEAN DEFAULT FALSE,
            avg_gap_days DOUBLE,
            PRIMARY KEY (site_id)
        )
    """),
    ("ndc_entries", """
        CREATE TABLE IF NOT EXISTS ndc_entries (
            ndc_code VARCHAR PRIMARY KEY,
            category_code VARCHAR NOT NULL,
            category_name VARCHAR NOT NULL,
            rc_category VARCHAR NOT NULL,
            rc_1 VARCHAR NOT NULL,
            rc_2 VARCHAR,
            title VARCHAR NOT NULL,
            differentiator TEXT,
            total_tickets INTEGER DEFAULT 0,
            sla_breach_pct DOUBLE,
            auto_resolve_pct DOUBLE,
            avg_mttr_min DOUBLE,
            median_mttr_min DOUBLE,
            pct_critical DOUBLE,
            pct_major DOUBLE,
            pct_minor DOUBLE,
            pct_low DOUBLE,
            typical_severity VARCHAR,
            escalation_pct DOUBLE,
            repeat_pct DOUBLE,
            calculated_priority VARCHAR,
            priority_score DOUBLE,
            pct_in_diamond DOUBLE,
            pct_in_platinum DOUBLE,
            pct_in_gold DOUBLE,
            pct_in_silver DOUBLE,
            pct_in_bronze DOUBLE,
            pct_in_3t DOUBLE,
            inap_match_pct DOUBLE,
            common_inap_misclass VARCHAR,
            status VARCHAR DEFAULT 'auto',
            reviewed_by VARCHAR,
            reviewed_at TIMESTAMP,
            notes TEXT,
            first_seen DATE,
            last_seen DATE,
            data_months INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_alarm_snapshot", """
        CREATE TABLE IF NOT EXISTS ndc_alarm_snapshot (
            ndc_code VARCHAR PRIMARY KEY,
            typical_severity VARCHAR,
            typical_ne_class VARCHAR,
            typical_fault_level VARCHAR,
            typical_impact VARCHAR,
            typical_type_ticket VARCHAR,
            typical_rat VARCHAR,
            peak_hours_range VARCHAR,
            peak_days VARCHAR,
            seasonal_pattern VARCHAR,
            avg_alarm_delay_desc VARCHAR,
            site_class_distribution VARCHAR,
            pct_3t DOUBLE,
            pct_kriteria DOUBLE,
            typical_device_age VARCHAR,
            top_regions VARCHAR,
            sample_size INTEGER,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_co_occurring_alarms", """
        CREATE TABLE IF NOT EXISTS ndc_co_occurring_alarms (
            id INTEGER PRIMARY KEY,
            ndc_code VARCHAR NOT NULL,
            co_alarm_description VARCHAR NOT NULL,
            co_alarm_rc_category VARCHAR,
            co_alarm_rc_1 VARCHAR,
            co_occurrence_pct DOUBLE NOT NULL,
            typical_lag_description VARCHAR,
            typical_lag_min DOUBLE,
            lag_direction VARCHAR,
            sample_size INTEGER,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_symptoms", """
        CREATE TABLE IF NOT EXISTS ndc_symptoms (
            id INTEGER PRIMARY KEY,
            ndc_code VARCHAR NOT NULL,
            symptom_text VARCHAR NOT NULL,
            symptom_type VARCHAR NOT NULL,
            frequency_pct DOUBLE,
            confidence VARCHAR DEFAULT 'medium',
            source VARCHAR,
            negative_note TEXT,
            redirect_ndc VARCHAR,
            sort_order INTEGER DEFAULT 0,
            is_auto_generated BOOLEAN DEFAULT TRUE,
            reviewed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_diagnostic_steps", """
        CREATE TABLE IF NOT EXISTS ndc_diagnostic_steps (
            id INTEGER PRIMARY KEY,
            ndc_code VARCHAR NOT NULL,
            step_number INTEGER NOT NULL,
            action TEXT NOT NULL,
            expected_result TEXT,
            if_yes TEXT,
            if_yes_goto_step INTEGER,
            if_no TEXT,
            if_no_goto_step INTEGER,
            if_no_redirect_ndc VARCHAR,
            avg_duration_min INTEGER,
            success_rate_at_step DOUBLE,
            cumulative_resolve_pct DOUBLE,
            is_auto_generated BOOLEAN DEFAULT FALSE,
            reviewed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_resolution_paths", """
        CREATE TABLE IF NOT EXISTS ndc_resolution_paths (
            id INTEGER PRIMARY KEY,
            ndc_code VARCHAR NOT NULL,
            path_name VARCHAR NOT NULL,
            sort_order INTEGER DEFAULT 0,
            probability_pct DOUBLE,
            avg_mttr_min DOUBLE,
            sla_met_pct DOUBLE,
            ticket_count INTEGER,
            notes TEXT,
            is_auto_generated BOOLEAN DEFAULT TRUE,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_resolution_steps", """
        CREATE TABLE IF NOT EXISTS ndc_resolution_steps (
            id INTEGER PRIMARY KEY,
            path_id INTEGER NOT NULL,
            step_number INTEGER NOT NULL,
            step_text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_escalation_matrix", """
        CREATE TABLE IF NOT EXISTS ndc_escalation_matrix (
            id INTEGER PRIMARY KEY,
            ndc_code VARCHAR NOT NULL,
            tier INTEGER NOT NULL,
            role VARCHAR NOT NULL,
            action TEXT NOT NULL,
            max_duration VARCHAR,
            sort_order INTEGER DEFAULT 0
        )
    """),
    ("ndc_preventive_actions", """
        CREATE TABLE IF NOT EXISTS ndc_preventive_actions (
            id INTEGER PRIMARY KEY,
            ndc_code VARCHAR NOT NULL,
            action TEXT NOT NULL,
            expected_impact TEXT,
            effort_level VARCHAR,
            sort_order INTEGER DEFAULT 0
        )
    """),
    ("ndc_confusion_matrix", """
        CREATE TABLE IF NOT EXISTS ndc_confusion_matrix (
            id INTEGER PRIMARY KEY,
            inap_rc_category VARCHAR,
            inap_rc_1 VARCHAR,
            confirmed_rc_category VARCHAR,
            confirmed_rc_1 VARCHAR,
            ticket_count INTEGER,
            match_pct DOUBLE,
            period_start DATE,
            period_end DATE,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("ndc_site_distribution", """
        CREATE TABLE IF NOT EXISTS ndc_site_distribution (
            site_id VARCHAR NOT NULL,
            ndc_code VARCHAR NOT NULL,
            period VARCHAR NOT NULL,
            ticket_count INTEGER,
            pct_of_site_total DOUBLE,
            avg_mttr_min DOUBLE,
            PRIMARY KEY (site_id, ndc_code, period)
        )
    """),
]

INDEXES_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_tickets_yearmonth ON noc_tickets(calc_year_month)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_site ON noc_tickets(site_id)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_area ON noc_tickets(calc_area_id)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_regional ON noc_tickets(calc_regional_id)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_source ON noc_tickets(calc_source)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_severity ON noc_tickets(severity)",
    "CREATE INDEX IF NOT EXISTS idx_weather_date ON ext_weather(date)",
    "CREATE INDEX IF NOT EXISTS idx_weather_province ON ext_weather(province)",
    "CREATE INDEX IF NOT EXISTS idx_pln_date ON ext_pln_outage(date)",
    "CREATE INDEX IF NOT EXISTS idx_pln_province ON ext_pln_outage(province)",
    "CREATE INDEX IF NOT EXISTS idx_annotations_date ON ext_annotations(date)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_category ON ndc_entries(category_code)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_priority ON ndc_entries(calculated_priority)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_rc ON ndc_entries(rc_1, rc_2)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_coalarm ON ndc_co_occurring_alarms(ndc_code)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_symptoms ON ndc_symptoms(ndc_code, symptom_type)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_diag ON ndc_diagnostic_steps(ndc_code, step_number)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_respath ON ndc_resolution_paths(ndc_code)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_resstep ON ndc_resolution_steps(path_id, step_number)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_esc ON ndc_escalation_matrix(ndc_code)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_prev ON ndc_preventive_actions(ndc_code)",
    "CREATE INDEX IF NOT EXISTS idx_ndc_sitedist ON ndc_site_distribution(site_id, ndc_code)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_rc ON noc_tickets(rc_category, rc_1, rc_2)",
]

VIEW_DDL = """
    CREATE OR REPLACE VIEW v_hierarchy AS
    SELECT
        t.to_id, t.to_name,
        n.nop_id, n.nop_name,
        r.regional_id, r.regional_name,
        a.area_id, a.area_name
    FROM master_to t
    JOIN master_nop n ON t.nop_id = n.nop_id
    JOIN master_regional r ON n.regional_id = r.regional_id
    JOIN master_area a ON r.area_id = a.area_id
    WHERE t.status = 'ACTIVE'
      AND n.status = 'ACTIVE'
      AND r.status = 'ACTIVE'
      AND a.status = 'ACTIVE'
"""

SEED_AREA = """
    INSERT INTO master_area (area_id, area_name, area_alias, status) VALUES
    ('AREA1', 'Area 1', 'AREA 1', 'ACTIVE'),
    ('AREA2', 'Area 2', 'AREA 2', 'ACTIVE'),
    ('AREA3', 'Area 3', 'AREA 3', 'ACTIVE'),
    ('AREA4', 'Area 4', 'AREA 4', 'ACTIVE')
    ON CONFLICT DO NOTHING
"""

SEED_SLA_TARGET = """
    INSERT INTO master_sla_target
      (id, area_id, site_class, site_flag, sla_target_pct, mttr_target_min, priority, description) VALUES
    (1, '*', '*',        '*',     90.0, 720,  0,  'Default target nasional'),
    (2, '*', 'Diamond',  '*',     95.0, 360,  10, 'Diamond class target'),
    (3, '*', 'Platinum', '*',     93.0, 480,  10, 'Platinum class target'),
    (4, '*', 'Gold',     '*',     90.0, 720,  10, 'Gold class target'),
    (5, '*', 'Silver',   '*',     88.0, 960,  10, 'Silver class target'),
    (6, '*', 'Bronze',   '*',     85.0, 1440, 10, 'Bronze class target'),
    (7, '*', '*',        '3T',    82.0, 2880, 20, '3T sites — akses sulit'),
    (8, '*', '*',        'USO/MP',80.0, 2880, 20, 'USO/MP sites'),
    (9, '*', '*',        'Femto', 80.0, 1440, 20, 'Femto sites')
    ON CONFLICT DO NOTHING
"""

SEED_THRESHOLD = """
    INSERT INTO master_threshold (param_key, param_value, param_unit, category, description) VALUES
    ('mttr_good',           240,   'minutes',    'MTTR',       'MTTR <= 4 jam = BAIK'),
    ('mttr_attention',      720,   'minutes',    'MTTR',       'MTTR <= 12 jam = PERHATIAN'),
    ('mttr_slow',           1440,  'minutes',    'MTTR',       'MTTR <= 24 jam = LAMBAT'),
    ('esc_normal',          3.0,   'percentage', 'ESCALATION', 'Eskalasi <= 3% = normal'),
    ('esc_warning',         7.0,   'percentage', 'ESCALATION', 'Eskalasi <= 7% = warning'),
    ('auto_resolve_good',   60.0,  'percentage', 'AUTO_RESOLVE','>=60% = efektif'),
    ('auto_resolve_moderate',40.0, 'percentage', 'AUTO_RESOLVE','>=40% = moderate'),
    ('repeat_normal',       10.0,  'percentage', 'REPEAT',     '<=10% = normal'),
    ('repeat_warning',      25.0,  'percentage', 'REPEAT',     '<=25% = warning'),
    ('vol_change_significant',10.0,'percentage', 'VOLUME',     'Perubahan >=10% = signifikan'),
    ('vol_change_alert',    15.0,  'percentage', 'VOLUME',     'Perubahan >=15% = alert'),
    ('trend_stable_sla',    0.5,   'pp/month',   'TREND',      'SLA |slope| < 0.5pp = stabil'),
    ('trend_stable_mttr',   5.0,   'pct/month',  'TREND',      'MTTR |slope| < 5% = stabil'),
    ('trend_stable_vol',    3.0,   'pct/month',  'TREND',      'Volume |slope| < 3% = stabil'),
    ('trend_decline_months',3,     'months',     'TREND',      'Declining >= 3 bln = serius'),
    ('anomaly_threshold',   2.0,   'z-score',    'ANOMALY',    '|z| > 2 = anomali'),
    ('anomaly_significant', 3.0,   'z-score',    'ANOMALY',    '|z| > 3 = signifikan'),
    ('risk_high',           70,    'score',      'RISK',       'Risk >= 70 = HIGH'),
    ('risk_medium',         40,    'score',      'RISK',       'Risk >= 40 = MEDIUM'),
    ('chronic_monthly_min', 20,    'tickets',    'BEHAVIOR',   'Chronic jika > 20 tiket/bln'),
    ('chronic_duration',    3,     'months',     'BEHAVIOR',   'Chronic jika 3+ bln berturut'),
    ('device_age_economic', 7,     'years',      'BEHAVIOR',   'Usia ekonomis perangkat'),
    ('seasonal_peak',       1.3,   'factor',     'SEASONAL',   'Peak jika > 1.3x rata-rata'),
    ('seasonal_low',        0.7,   'factor',     'SEASONAL',   'Low jika < 0.7x rata-rata'),
    ('capacity_buffer',     0.9,   'factor',     'CAPACITY',   'Alert saat > 90% kapasitas'),
    ('pattern_cv_consistent',0.5,  'cv',         'PATTERN',    'CV < 0.5 = pola konsisten'),
    ('max_recommendations',  5,    'count',      'DISPLAY',    'Maks rekomendasi per Report Card'),
    ('cache_ttl_minutes',    5,    'minutes',    'CACHE',      'Session cache TTL'),
    ('cost_hour_diamond',    5000000, 'Rp/jam',  'COST',       'Estimasi kerugian/jam Diamond'),
    ('cost_hour_platinum',   3000000, 'Rp/jam',  'COST',       'Estimasi kerugian/jam Platinum'),
    ('cost_hour_gold',       1500000, 'Rp/jam',  'COST',       'Estimasi kerugian/jam Gold'),
    ('cost_hour_silver',     750000,  'Rp/jam',  'COST',       'Estimasi kerugian/jam Silver'),
    ('cost_hour_bronze',     250000,  'Rp/jam',  'COST',       'Estimasi kerugian/jam Bronze'),
    ('action_eval_window',   90,    'days',      'ACTION',     'Window before-after evaluasi'),
    ('action_decay_months',  3,     'months',    'ACTION',     'Berapa bulan pantau decay'),
    ('action_success_pct',   30,    'percentage','ACTION',     'Penurunan >=30% = berhasil'),
    ('compare_normalize',    1,     'boolean',   'COMPARE',    'Normalisasi per 100 site'),
    ('compare_min_tickets',  50,    'tickets',   'COMPARE',    'Min tiket untuk compare valid')
    ON CONFLICT DO NOTHING
"""

ALL_TABLE_NAMES = [name for name, _ in TABLES_DDL]


def _migrate_saved_views(conn):
    try:
        cols = [r[0] for r in conn.execute("DESCRIBE saved_views").fetchall()]
        if "entity_name" not in cols:
            conn.execute("DROP TABLE IF EXISTS saved_views")
            for name, ddl in TABLES_DDL:
                if name == "saved_views":
                    conn.execute(ddl)
                    break
            logger.info("Migrated saved_views table to new schema")
    except Exception:
        pass

    try:
        cols = [r[0] for r in conn.execute("DESCRIBE report_history").fetchall()]
        if "pdf_path" not in cols:
            conn.execute("DROP TABLE IF EXISTS report_history")
            for name, ddl in TABLES_DDL:
                if name == "report_history":
                    conn.execute(ddl)
                    break
            logger.info("Migrated report_history table to new schema")
    except Exception:
        pass


def initialize_schema():
    tables_created = []
    with get_write_connection() as conn:
        _migrate_saved_views(conn)
        for name, ddl in TABLES_DDL:
            conn.execute(ddl)
            tables_created.append(name)

        for idx_sql in INDEXES_DDL:
            conn.execute(idx_sql)

        conn.execute(VIEW_DDL)

        conn.execute(SEED_AREA)
        conn.execute(SEED_SLA_TARGET)
        conn.execute(SEED_THRESHOLD)

    from backend.services.calendar_service import seed_calendar_if_empty
    seed_calendar_if_empty()

    seed_counts = _get_seed_counts()
    logger.info(f"Schema initialized: {len(tables_created)} tables created")

    return {
        "tables_created": tables_created,
        "seed_data": seed_counts,
        "status": "success",
    }


def reset_seed_data():
    with get_write_connection() as conn:
        conn.execute("DELETE FROM master_threshold")
        conn.execute("DELETE FROM master_sla_target")
        conn.execute(SEED_SLA_TARGET)
        conn.execute(SEED_THRESHOLD)

    return {"reset": ["master_threshold", "master_sla_target"]}


def get_schema_status():
    tables = {}
    with get_connection() as conn:
        for name in ALL_TABLE_NAMES:
            try:
                result = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()
                tables[name] = {"exists": True, "rows": result[0]}
            except Exception:
                tables[name] = {"exists": False, "rows": 0}

    all_exist = all(t["exists"] for t in tables.values())
    return {
        "tables": tables,
        "total_tables": len(ALL_TABLE_NAMES),
        "initialized": all_exist,
    }


def _get_seed_counts():
    counts = {}
    with get_connection() as conn:
        for name in ["master_area", "master_sla_target", "master_threshold"]:
            try:
                result = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()
                counts[name] = result[0]
            except Exception:
                counts[name] = 0
    return counts
