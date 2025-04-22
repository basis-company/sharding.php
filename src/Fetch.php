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
            if (Bucket::isDedicated($buckets[0])) {
                [$_, $table] = explode('_', $table, 2);
            }
            $driver = $this->database->getStorageDriver($storageId);
            $rows = $callback($driver, $table, $buckets);
            foreach ($rows as $row) {
                $result[] = $this->database->createInstance(
                    class: $tableClass ?: $this->class,
                    row: $row,
                    isDedicated: Bucket::isDedicated($buckets[0]),
                );
                if ($this->first) {
                    return array_pop($result);
                }
            }
        }

        return $this->first ? null : $result;
    }
}
