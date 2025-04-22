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
        public bool $first = false,
    ) {
    }

    public function first(bool $first = true): self
    {
        $this->first = $first;
        return $this;
    }

    public function from(array $buckets, $create = false, bool $single = false): self
    {
        if (array_is_list($buckets) && count($buckets) && $buckets[0] instanceof Bucket) {
            $this->buckets = $buckets;
        } else {
            $this->buckets = $this->database->locate($this->class, $buckets, $create, $single);
        }
        return $this;
    }

    public function using(callable $callback): array|object|null
    {
        $dedicatedBuckets = [];
        if (!$this->buckets) {
            $this->buckets = $this->database->locate($this->class);
        }

        foreach ($this->buckets as $bucket) {
            if (!$bucket->storage) {
                continue;
            }
            $isDedicated = Bucket::isDedicated($bucket) ? 1 : 0;

            if (!array_key_exists($isDedicated, $dedicatedBuckets)) {
                $dedicatedBuckets[$isDedicated] = [];
            }

            if (!array_key_exists($bucket->storage, $dedicatedBuckets[$isDedicated])) {
                $dedicatedBuckets[$isDedicated][$bucket->storage] = [];
            }

            $dedicatedBuckets[$isDedicated][$bucket->storage][] = $bucket;
        }

        $result = [];

        foreach ($dedicatedBuckets as $isDedicated => $storageBuckets) {
            $tableClass = null;
            if (!class_exists($this->class)) {
                $table = str_replace('.', '_', $this->class);
                if ($this->database->schema->hasTable($table)) {
                    $tableClass = $this->database->schema->getTableClass($table);
                }
            } else {
                $table = $this->database->schema->getClassTable($this->class);
            }
            if ($isDedicated) {
                [$_, $table] = explode('_', $table, 2);
            }
            foreach ($storageBuckets as $storageId => $buckets) {
                $driver = $this->database->getStorageDriver($storageId);
                foreach ($callback($driver, $table, $buckets) as $row) {
                    $result[] = $this->database->createInstance(
                        class: $tableClass ?: $this->class,
                        row: $row,
                        isDedicated: boolval($isDedicated)
                    );
                    if ($this->first) {
                        return array_pop($result);
                    }
                }
            }
        }

        return $this->first ? null : $result;
    }
}
