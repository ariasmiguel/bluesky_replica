CREATE TABLE IF NOT EXISTS posts
(
    uri String,
    author_handle String,
    content String,
    created_at DateTime,
    like_count UInt32,
    repost_count UInt32
)
ENGINE = MergeTree()
ORDER BY (created_at, uri)
PARTITION BY toYYYYMM(created_at); 