-- ─────────────────────────────────────────────────────────────
-- Kannel Reporting Schema
-- Apply: mysql -u root sms < test/schema.sql
-- ─────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS sms CHARACTER SET utf8mb4;
USE sms;

-- ── MT message log ───────────────────────────────────────────
-- Written by your app on sendsms HTTP 200 response.
-- msg_id = UUID returned by Kannel in response body.
CREATE TABLE IF NOT EXISTS mt_log (
    msg_id      CHAR(36)        NOT NULL,
    account     VARCHAR(64)     NOT NULL DEFAULT '',
    src         VARCHAR(32)     NOT NULL DEFAULT '',
    dst         VARCHAR(32)     NOT NULL DEFAULT '',
    smsc        VARCHAR(32)     NOT NULL DEFAULT '',
    status      TINYINT UNSIGNED NOT NULL DEFAULT 0,
    -- 0=submitted, 1=delivered, 2=failed, 4=buffered, 8=smsc_ok, 16=smsc_fail
    submitted_at DATETIME       NOT NULL DEFAULT NOW(),
    updated_at  DATETIME        NOT NULL DEFAULT NOW() ON UPDATE NOW(),
    PRIMARY KEY (msg_id),
    INDEX idx_account_submitted (account, submitted_at),
    INDEX idx_dst_submitted     (dst, submitted_at),
    INDEX idx_status            (status)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- ── DLR report ───────────────────────────────────────────────
-- Written by dlr_writer.py via Redis Stream.
-- msg_id correlates to mt_log.msg_id.
CREATE TABLE IF NOT EXISTS dlr_report (
    msg_id      CHAR(36)        NOT NULL,
    foreign_id  VARCHAR(64)     NOT NULL DEFAULT '',  -- SMSC-assigned ID (%F)
    status      TINYINT UNSIGNED NOT NULL,
    smsc        VARCHAR(32)     NOT NULL DEFAULT '',
    src         VARCHAR(32)     NOT NULL DEFAULT '',
    dst         VARCHAR(32)     NOT NULL DEFAULT '',
    account     VARCHAR(64)     NOT NULL DEFAULT '',
    ts          INT UNSIGNED    NOT NULL DEFAULT 0,
    created_at  DATETIME        NOT NULL DEFAULT NOW(),
    updated_at  DATETIME        NOT NULL DEFAULT NOW() ON UPDATE NOW(),
    PRIMARY KEY (msg_id),
    INDEX idx_dst_ts    (dst, ts),
    INDEX idx_account   (account, ts),
    INDEX idx_status    (status),
    INDEX idx_foreign   (foreign_id)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

-- ── Reporting views ──────────────────────────────────────────

-- Delivery summary per account per hour
CREATE OR REPLACE VIEW v_delivery_hourly AS
SELECT
    account,
    DATE_FORMAT(FROM_UNIXTIME(ts), '%Y-%m-%d %H:00') AS hour,
    COUNT(*)                                           AS total,
    SUM(status = 1)                                    AS delivered,
    SUM(status = 2)                                    AS failed,
    SUM(status = 4)                                    AS buffered,
    ROUND(100.0 * SUM(status = 1) / COUNT(*), 2)       AS delivery_pct
FROM dlr_report
GROUP BY account, hour;

-- Undelivered messages (submitted but no final DLR yet)
CREATE OR REPLACE VIEW v_pending AS
SELECT m.msg_id, m.account, m.src, m.dst, m.smsc, m.submitted_at
FROM mt_log m
LEFT JOIN dlr_report d ON d.msg_id = m.msg_id
WHERE d.msg_id IS NULL
   OR d.status NOT IN (1, 2);
