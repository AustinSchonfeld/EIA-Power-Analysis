USE Energy;
GO

CREATE SCHEMA stg;
GO

CREATE TABLE stg.eia_hourly_generation_bytype_raw(
	id BIGINT IDENTITY PRIMARY KEY,
	batch_id VARCHAR(36) NOT NULL,
	loaded_at DATETIME2	 NOT NULL DEFAULT SYSUTCDATETIME(),
	payload NVARCHAR(4000) NOT NULL CHECK (ISJSON(payload) = 1)
	);

CREATE INDEX ix_eia_raw_batch ON stg.eia_hourly_generation_bytype_raw (batch_id);

CREATE TABLE dbo.eia_hourly_generation_bytype(
	period	DATETIME2(0)	NOT NULL,
	respondent VARCHAR (20) NOT NULL,
	respondent_name VARCHAR (50) NOT NULL,
	fueltype VARCHAR (20) NOT NULL,
	fuel_name VARCHAR (50) NOT NULL,
	value INT,
	value_units VARCHAR (30) NOT NULL,
	updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
	CONSTRAINT pk_eia_hourly_bytype PRIMARY KEY (period, respondent, fueltype)
	);