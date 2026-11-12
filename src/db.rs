use rusqlite::Connection;

pub fn init_db(path: &str) -> Result<Connection, rusqlite::Error> {
    let conn = if path == ":memory:" {
        Connection::open_in_memory()?
    } else {
        Connection::open(path)?
    };

    // Tune SQLite for low memory footprint and fast concurrent reads/writes
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -2000;
        PRAGMA busy_timeout = 5000;

        CREATE TABLE IF NOT EXISTS polls (
            id TEXT PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            admin_key TEXT NOT NULL UNIQUE,
            password TEXT,
            status TEXT NOT NULL DEFAULT 'OPEN',
            expires_at TEXT,
            captcha_required INTEGER NOT NULL DEFAULT 0,
            results_visibility TEXT NOT NULL DEFAULT 'PUBLIC',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS poll_options (
            id TEXT PRIMARY KEY NOT NULL,
            poll_id TEXT NOT NULL,
            option_text TEXT NOT NULL,
            display_order INTEGER NOT NULL,
            vote_count INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_poll_options_poll_id
            ON poll_options(poll_id, display_order);

        CREATE TABLE IF NOT EXISTS votes (
            id TEXT PRIMARY KEY NOT NULL,
            poll_id TEXT NOT NULL,
            option_id TEXT NOT NULL,
            voter_hash TEXT NOT NULL,
            voted_at TEXT NOT NULL,
            FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE,
            FOREIGN KEY (option_id) REFERENCES poll_options(id) ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_poll_voter
            ON votes(poll_id, voter_hash);
        ",
    )?;

    Ok(conn)
}
