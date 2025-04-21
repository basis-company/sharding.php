<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Entity\Bucket;

class Fetch
{
    public array $buckets = [];

    public function __construct(
        public readonly Database $database,
        public readonly string $class,
        public bool $one = false,
    ) {
    }

    public function from(array $buckets): self
    {
        $this->buckets = $buckets;
        return $this;
    }

    public function using(callable $callback): array|object|null
    {
        $storageBuckets = [];
        foreach ($this->buckets as $bucket) {
            if (!$bucket->storage) {
                continue;
            }
            if (!array_key_exists($bucket->storage, $storageBuckets)) {
                $storageBuckets[$bucket->storage] = [$bucket];
            } else {
                $storageBuckets[$bucket->storage][] = $bucket;
            }
        }

        $result = [];
        foreach ($storageBuckets as $storageId => $buckets) {
            $tableClass = null;
            if (!class_exists($this->class)) {
                $table = str_replace('.', '_', $this->class);
                if ($this->database->meta->hasTable($table)) {
                    $tableClass = $this->database->meta->getTableClass($table);
                }
            } else {
                $table = $this->database->meta->getClassTable($this->class);
            }
            $prefixPresent = $buckets[0]->flags & Bucket::DROP_PREFIX_FLAG;
            if ($prefixPresent) {
                [$_, $table] = explode('_', $table, 2);
            }
            $driver = $this->database->getStorageDriver($storageId);
            $rows = $callback($driver, $table, $buckets);
            foreach ($rows as $row) {
                $result[] = $this->database->createInstance($tableClass ?: $this->class, $row, !$prefixPresent);
            }
        }

        return $this->one ? (count($result) ? $result[0] : null) : $result;
    }
}
