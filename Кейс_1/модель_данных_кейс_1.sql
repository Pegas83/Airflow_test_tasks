CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS src;

-- src.t_employee определение

CREATE TABLE IF NOT EXISTS src.t_employee (
	department text NULL,
	"position" text NULL,
	tab_num text NULL,
	fio text NULL,
	birth_date date NULL,
	address text NULL,
	phone_1 text NULL,
	phone_2 text NULL,
	work_month text NULL,
	hours_worked int4 NULL
);

-- stg.t_employee определение

CREATE TABLE IF NOT EXISTS stg.t_employee (
	tab_num_work_month_key text NOT NULL,
	department text NULL,
	"position" text NULL,
	tab_num text NOT NULL,
	fio text NULL,
	birth_date date NULL,
	address text NULL,
	phone_1 text NULL,
	phone_2 text NULL,
	work_month text NULL,
	hours_worked int4 NULL,
	deleted timestamp NULL,
	last_update timestamp NOT NULL
);

-- stg.upd_dttm определение

CREATE TABLE IF NOT EXISTS stg.upd_dttm (
	upd_dttm timestamp NULL
);

-- core.t_employee определение

CREATE TABLE IF NOT EXISTS core.t_employee (
	tab_num_work_month_key text NOT NULL,
	department text NULL,
	"position" text NULL,
	tab_num text NULL,
	fio text NULL,
	birth_date date NULL,
	address text NULL,
	phone_1 text NULL,
	phone_2 text NULL,
	work_month text NULL,
	hours_worked int4 NULL,
	effective_date_from timestamp NOT NULL,
	effective_date_to timestamp NOT NULL,
	is_active bool DEFAULT true NOT NULL
);

TRUNCATE core.t_employee;
TRUNCATE stg.t_employee;
TRUNCATE src.t_employee;
TRUNCATE stg.upd_dttm;

CREATE OR REPLACE PROCEDURE stg.t_employee_load() AS 
$$
	DECLARE 
		time timestamp = now();
BEGIN
--определяем удаленные записи
	UPDATE
		stg.t_employee stg
	SET 
		last_update = time,
		deleted = time
	WHERE
		stg.tab_num_work_month_key NOT IN(
		SELECT
			md5(COALESCE(tab_num,'')|| COALESCE(work_month,''))
		FROM
			src.t_employee
		);
--вставляем неудаленные записи
	INSERT INTO	stg.t_employee (
		tab_num_work_month_key,
		department,
		"position",
		tab_num,
		fio,
		birth_date,
		address,
		phone_1,
		phone_2,
		work_month,
		hours_worked,
		last_update
		)
	SELECT
		md5(COALESCE(tab_num,'')|| COALESCE(work_month,'')),
		department,
		"position",
		tab_num,
		fio,
		birth_date,
		address,
		phone_1,
		phone_2,
		work_month,
		hours_worked,
		now()
	FROM
		src.t_employee;
--
	INSERT INTO stg.upd_dttm VALUES (time);
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE core.t_employee_load() AS 
$$
BEGIN
		--помечаем удаленные записи
	UPDATE
		core.t_employee core
	SET 
		is_active = FALSE,
		effective_date_to = stg.deleted
	FROM
		stg.t_employee stg
	WHERE
		stg.deleted IS NOT NULL
		AND core.tab_num_work_month_key = stg.tab_num_work_month_key
		AND core.is_active IS TRUE
	;
		--получаем идентификаторы новых строк
	CREATE TEMPORARY TABLE new_pk_list ON
	COMMIT DROP AS 
	SELECT
		stg.tab_num_work_month_key
	FROM
		stg.t_employee stg
	LEFT JOIN core.t_employee core
		ON
		core.tab_num_work_month_key = stg.tab_num_work_month_key
	WHERE
		core.tab_num_work_month_key IS NULL
	;
--добавляем новые строки в core  
	INSERT INTO	core.t_employee(
		tab_num_work_month_key,
		department,
		"position",
		tab_num,
		fio,
		birth_date,
		address,
		phone_1,
		phone_2,
		work_month,
		hours_worked,
		effective_date_from,
		effective_date_to
	)
	SELECT
		tab_num_work_month_key,
		department,
		"position",
		tab_num,
		fio,
		birth_date,
		address,
		phone_1,
		phone_2,
		work_month,
		hours_worked,
		stg.last_update/*'1900-01-01':: date */AS effective_date_from,
		COALESCE(stg.deleted,'9999-01-01') AS effective_date_to
	FROM
		stg.t_employee stg
	JOIN new_pk_list npk
		USING(tab_num_work_month_key)
	;
-- помечаем измененные данные не активными
	UPDATE
		core.t_employee core
	SET 
		is_active = FALSE,
		effective_date_to = stg.last_update
	FROM
		stg.t_employee stg
	LEFT JOIN new_pk_list npk
		USING (tab_num_work_month_key)
	WHERE
		npk.tab_num_work_month_key IS NULL
		AND stg.deleted IS NULL
		AND core.tab_num_work_month_key = stg.tab_num_work_month_key
		AND core.is_active IS TRUE
	;
--добавляем актуальные строки для измененных строк
	INSERT INTO core.t_employee(
		tab_num_work_month_key,
		department,
		"position",
		tab_num,
		fio,
		birth_date,
		address,
		phone_1,
		phone_2,
		work_month,
		hours_worked,
		effective_date_from,
		effective_date_to
	)
	SELECT
		tab_num_work_month_key,
		department,
		"position",
		tab_num,
		fio,
		birth_date,
		address,
		phone_1,
		phone_2,
		work_month,
		hours_worked,
		stg.last_update AS effective_date_from,
		'9999-01-01':: date AS effective_date_to
	FROM
		stg.t_employee stg
	LEFT JOIN new_pk_list npk
		USING(tab_num_work_month_key)
	WHERE
		npk.tab_num_work_month_key IS NULL
		AND stg.deleted IS NULL
		AND stg.last_update = (
		SELECT
			max(upd_dttm)
		FROM
			stg.upd_dttm)
	;
	TRUNCATE src.t_employee;
END;
$$
LANGUAGE plpgsql;