CREATE TABLE IF NOT EXISTS workers (
    worker_id text PRIMARY KEY,
    version text NOT NULL DEFAULT 'unknown',
    hostname text NOT NULL DEFAULT '',
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scraping_tasks (
    task_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_key text NOT NULL UNIQUE,
    target_url text NOT NULL,
    task_type text NOT NULL DEFAULT 'text_search',
    search_keyword text NOT NULL DEFAULT '',
    search_location text NOT NULL DEFAULT '',
    order_id text NOT NULL DEFAULT '',
    order_ids text[] NOT NULL DEFAULT '{}',
    query_type text NOT NULL DEFAULT 'Unknown',
    minimum_reviews integer NOT NULL DEFAULT 0 CHECK (minimum_reviews >= 0),
    priority integer NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    worker_id text REFERENCES workers(worker_id),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL DEFAULT 4 CHECK (max_attempts > 0),
    lease_until timestamptz,
    raw_file_path text,
    last_error text,
    parse_status text NOT NULL DEFAULT 'PENDING'
        CHECK (parse_status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    parse_attempt_count integer NOT NULL DEFAULT 0,
    parse_lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    parsed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS scraping_tasks_acquire_idx
    ON scraping_tasks (priority DESC, created_at, task_id)
    WHERE status IN ('PENDING', 'PROCESSING');
CREATE INDEX IF NOT EXISTS scraping_tasks_parse_idx
    ON scraping_tasks (created_at, task_id)
    WHERE status = 'COMPLETED' AND parse_status IN ('PENDING', 'PROCESSING');

CREATE TABLE IF NOT EXISTS leads (
    id text PRIMARY KEY,
    task_id uuid REFERENCES scraping_tasks(task_id),
    search_keyword text NOT NULL DEFAULT '',
    search_location text NOT NULL DEFAULT '',
    order_ids text[] NOT NULL DEFAULT '{}',
    query_type text NOT NULL DEFAULT 'Unknown',
    name text NOT NULL DEFAULT '',
    url text NOT NULL DEFAULT '',
    website text NOT NULL DEFAULT '',
    rating double precision,
    reviews integer NOT NULL DEFAULT 0,
    category text NOT NULL DEFAULT '',
    phone text NOT NULL DEFAULT '',
    phone_type text NOT NULL DEFAULT '',
    scraped_date timestamptz,
    emails text NOT NULL DEFAULT '',
    alternative_phones text NOT NULL DEFAULT '',
    linkedin text NOT NULL DEFAULT '',
    facebook text NOT NULL DEFAULT '',
    twitter text NOT NULL DEFAULT '',
    instagram text NOT NULL DEFAULT '',
    data_status text NOT NULL DEFAULT 'RAW',
    found_web_mobile text NOT NULL DEFAULT 'No',
    is_valid boolean NOT NULL DEFAULT true,
    lead_status text NOT NULL DEFAULT '',
    first_seen_date timestamptz,
    last_updated_date timestamptz,
    dead_email boolean NOT NULL DEFAULT false,
    enriched_at timestamptz,
    enrichment_version integer,
    http_status integer,
    domain_state text NOT NULL DEFAULT '',
    ssl_status text NOT NULL DEFAULT '',
    mobile_friendly boolean NOT NULL DEFAULT false,
    has_contact_info boolean NOT NULL DEFAULT false,
    quality_score integer NOT NULL DEFAULT 0,
    tags jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_checked_at timestamptz,
    raw_extracted jsonb NOT NULL DEFAULT '{}'::jsonb,
    enrichment_lease_until timestamptz
);

CREATE INDEX IF NOT EXISTS leads_enrichment_idx
    ON leads (id)
    WHERE data_status = 'RAW';
CREATE INDEX IF NOT EXISTS leads_task_idx ON leads (task_id);

CREATE TABLE IF NOT EXISTS system_config (
    key text PRIMARY KEY,
    value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO system_config (key, value)
VALUES ('target_version', 'v1.0.0')
ON CONFLICT (key) DO NOTHING;
