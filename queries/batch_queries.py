
get_checkpoint_query = """
SELECT last_system_timestamp 
FROM checkpoints.ingestion_checkpoints 
WHERE pipeline_name = 'item_prices_ingestion'
ORDER BY updated_at DESC 
LIMIT 1
"""

check_table_query = """
SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables 
    WHERE table_name = 'ingestion_checkpoints'
);
"""

update_checkpoint_query="""
INSERT INTO checkpoints.ingestion_checkpoints (pipeline_name, last_system_timestamp)
VALUES ('item_prices_ingestion', %s)
"""

create_and_insert_query = """
CREATE TABLE checkpoints.ingestion_checkpoints (
    id SERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    last_system_timestamp TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO checkpoints.ingestion_checkpoints (pipeline_name, last_system_timestamp)
VALUES ('item_prices_ingestion', '1900-01-01T01:01:00+01:00')
"""

create_or_truncate_staging_item_table_query="""
CREATE TABLE IF NOT EXISTS public.item_prices_staging (
    id UUID PRIMARY KEY,
    item TEXT,
    price NUMERIC,
    currency TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    system_timestamp TIMESTAMPTZ
);

TRUNCATE TABLE public.item_prices_staging;
"""

insert_df_into_staging_query = """
INSERT INTO public.item_prices_staging (
    id, item, price, currency, created_at, updated_at, system_timestamp
)
VALUES %s
"""

create_item_table_query = """
CREATE TABLE IF NOT EXISTS public.item_prices (
    id UUID PRIMARY KEY,
    item TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    currency TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    system_timestamp TIMESTAMPTZ NOT NULL
);
"""

upsert_item_price_query = """
INSERT INTO public.item_prices AS target (
    id, item, price, currency, created_at, updated_at, system_timestamp
)
SELECT 
    id, item, price, currency, created_at, updated_at, system_timestamp
FROM public.item_prices_staging
ON CONFLICT (id)
DO UPDATE SET
    item = EXCLUDED.item,
    price = EXCLUDED.price,
    currency = EXCLUDED.currency,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    system_timestamp = EXCLUDED.system_timestamp
WHERE target.system_timestamp < EXCLUDED.system_timestamp;
"""

