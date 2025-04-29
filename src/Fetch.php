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
        if (!$this->buckets) {
            $this->buckets = $this->database->locate($this->class);
        }

        $rows = [];
        foreach ($this->buckets as $bucket) {
            $tableClass = null;
            if (!class_exists($this->class)) {
                $table = str_replace('.', '_', $this->class);
                if ($this->database->schema->hasTable($table)) {
                    $tableClass = $this->database->schema->getTableClass($table);
                }
            } else {
                $table = $this->database->schema->getClassTable($this->class);
            }
            if (Bucket::isDedicated($bucket)) {
                [$_, $table] = explode('_', $table, 2);
            }
            if (!$bucket->storage) {
                continue;
            }
            $driver = $this->database->getStorageDriver($bucket->storage);
            foreach ($callback($driver, $table) as $row) {
                $rows[] = $this->database->createInstance(
                    class: $tableClass ?: $this->class,
                    row: $row,
                );
                if ($this->first) {
                    return array_pop($rows);
                }
            }
        }

        return $this->first ? null : $rows;
    }
}
