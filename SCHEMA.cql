CREATE KEYSPACE objects WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
};

USE objects;

CREATE TABLE object_chunks (
  objectname text,
  updated bigint,
  nodetag uuid,
  chunk_num bigint,
  checksum text,
  payload blob,
  PRIMARY KEY ((objectname, updated, nodetag, chunk_num))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=300 AND
  index_interval=128 AND
  read_repair_chance=0.050000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='200.0ms' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'sstable_size_in_mb': '1000', 'tombstone_compaction_interval': '0', 'class': 'LeveledCompactionStrategy'} AND
  compression={};

CREATE TABLE objects (
  objectname text,
  updated bigint,
  nodetag uuid,
  checksum text,
  chunk_size bigint,
  num_chunks bigint,
  object_size bigint,
  path text,
  PRIMARY KEY ((objectname), updated, nodetag)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.050000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='200.0ms' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE INDEX objects_path ON objects (path);

CREATE TABLE paths (
  path text,
  parent text,
  PRIMARY KEY ((path))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.050000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='200.0ms' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};
