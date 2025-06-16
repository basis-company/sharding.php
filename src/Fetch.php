<?php

declare(strict_types=1);

namespace Basis\Sharding;

use Basis\Sharding\Entity\Bucket;
use Exception;

class Fetch
{
    public array $buckets = [];

    public function __construct(
        public readonly Database $database,
        public readonly ?string $class = null,
        public bool $first = false,
    ) {
    }

    public function first(bool $first = true): self
    {
        $this->first = $first;
        return $this;
    }

    public function from(array $buckets, bool $writable = false, bool $multiple = true): self
    {
        if (array_is_list($buckets) && count($buckets) && $buckets[0] instanceof Bucket) {
            $this->buckets = $buckets;
        } else {
            $this->buckets = $this->database->getBuckets($this->class, $buckets, $writable, $multiple);
        }
        return $this;
    }

    public function query(string $query, array $params = []): array
    {
        if (!$this->buckets) {
            throw new Exception("No buckets defined");
        }

        $result = [];

        foreach ($this->buckets as $bucket) {
            $driver = $this->database->getStorageDriver($bucket->storage);
            $bucketResult = $driver->query($query, $params);
            if (array_is_list($bucketResult) && count($bucketResult)) {
                foreach ($bucketResult as $row) {
                    $result[] = $row;
                }
            } else {
                $result = array_merge($result, $bucketResult);
            }
            if ($this->first && count($result)) {
                if (array_is_list($result)) {
                    return $result[0];
                }
                return $result;
            }
        }

        return $result;
    }

    public function using(callable $callback): array|object|null
    {
        if (!$this->class) {
            throw new Exception("No class defined");
        }

        if (!$this->buckets) {
            $this->buckets = $this->database->getBuckets($this->class);
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
            if ($bucket->isDedicated()) {
                $table = explode('_', $table, 2)[1];
            }
            if (!$bucket->storage) {
                continue;
            }
            $driver = $this->database->getStorageDriver($bucket->storage);
            foreach ($callback($driver, $table) as $row) {
                $rows[] = $this->database->factory->getInstance(
                    class: $tableClass ?: $this->class,
                    data: $row,
                );
                if ($this->first) {
                    return array_pop($rows);
                }
            }
        }

        return $this->first ? null : $rows;
    }
}
