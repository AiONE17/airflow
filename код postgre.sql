CREATE SCHEMA report;

DROP TABLE IF EXISTS report.sridonbuftoload;

CREATE TABLE IF NOT EXISTS report.sridonbuftoload
(
    dt_last_load TIMESTAMP,
    dt_hour      TIMESTAMP NOT NULL,
    qty          INTEGER,
    PRIMARY KEY (dt_hour)
);

CREATE SCHEMA whsync;

CREATE OR REPLACE PROCEDURE whsync.sridonbuftoload(_src JSONB)
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    SET TIME ZONE 'Europe/Moscow';

    WITH cte AS (
        SELECT DISTINCT ON (src.dt_hour) dt_last_load,
                                         dt_hour,
                                         qty
        FROM JSONB_TO_RECORDSET(_src) AS src(dt_last_load TIMESTAMP WITHOUT TIME ZONE,
                                             dt_hour      TIMESTAMP WITHOUT TIME ZONE,
                                             qty          INTEGER)
        ORDER BY src.dt_hour,src.dt_last_load DESC)

    INSERT
    INTO report.sridonbuftoload AS psh(dt_last_load,
                                       dt_hour,
                                       qty)
    SELECT c.dt_last_load,
           c.dt_hour,
           c.qty
    FROM cte c
    ON CONFLICT (dt_hour) DO UPDATE
        SET dt_last_load = excluded.dt_last_load,
            qty          = excluded.qty
    WHERE psh.dt_last_load < excluded.dt_last_load;
END;
$$;
