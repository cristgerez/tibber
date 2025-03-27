create_temp_currencies_table = """
CREATE TEMPORARY TABLE staging_currencies (
    code TEXT PRIMARY KEY, 
    name TEXT NOT NULL, 
    symbol TEXT
);
"""

create_temp_currency_convertion_rates_table = """
CREATE TEMPORARY TABLE staging_currency_conversion_rates (
    base_currency TEXT,
    target_currency TEXT,
    exchange_rate NUMERIC NOT NULL,
    PRIMARY KEY (base_currency, target_currency)
);
"""

create_currencies_table = """
CREATE TABLE IF NOT EXISTS currencies (
    code TEXT PRIMARY KEY, 
    name TEXT NOT NULL, 
    symbol TEXT
);
"""

create_currency_convertion_rates_table = """
CREATE TABLE IF NOT EXISTS currency_conversion_rates (
    base_currency TEXT REFERENCES currencies(code),
    target_currency TEXT REFERENCES currencies(code),
    exchange_rate NUMERIC NOT NULL,
    PRIMARY KEY (base_currency, target_currency)
);
"""

upsert_currencies = """
    INSERT INTO currencies (code, name, symbol)
    SELECT code, name, symbol FROM staging_currencies
    ON CONFLICT (code)
    DO UPDATE SET name = EXCLUDED.name, symbol = EXCLUDED.symbol;
"""

upsert_currency_conversion_rates = """
    INSERT INTO currency_conversion_rates (base_currency, target_currency, exchange_rate)
    SELECT base_currency, target_currency, exchange_rate FROM staging_currency_conversion_rates
    ON CONFLICT (base_currency, target_currency)
    DO UPDATE SET exchange_rate = EXCLUDED.exchange_rate;
"""