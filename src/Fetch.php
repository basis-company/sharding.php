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
        public ?string $class = null,
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

    public function query(string $query, array $params = []): array|object|null
    {
        if (!$this->buckets && $this->class) {
            $this->from([]);
        }
        if (!$this->buckets) {
            throw new Exception("No buckets defined");
        }

        $result = [];

        foreach ($this->buckets as $bucket) {
            $driver = $bucket->isCore() ? $this->database->getCoreDriver() : $this->database->getStorage($bucket->storage)->getDriver();
            $bucketResult = $driver->query($query, $params);
            if (array_is_list($bucketResult) && count($bucketResult)) {
                foreach ($bucketResult as $row) {
                    $result[] = $row;
                }
            } else {
                $result = array_merge($result, $bucketResult);
            }
            if ($this->first && count($result)) {
                break;
            }
        }
        if ($this->class) {
            if (!$this->first) {
                // query result is array of tuples
                $result = $result[0];
            }
            $result = array_map(fn($row) => $this->database->factory->getInstance($this->class, $row), $result);
        }

        if ($this->first) {
            return count($result) ? $result[0] : null;
        }

        return $result;
    }

    public function using(callable $callback): array|object|null
    {
        if (!$this->class) {
            throw new Exception("No class defined");
        }

        $class = str_replace('.', '_', $this->class);
        if (!class_exists($class, false)) {
            if ($this->database->schema->hasTable($class)) {
                $class = $this->database->schema->getTableClass($class);
            }
        }

        if (!$this->buckets) {
            $this->buckets = $this->database->getBuckets($class);
        }

        $rows = [];
        foreach ($this->buckets as $bucket) {
            if (!$bucket->storage) {
                continue;
            }
            $table = class_exists($class) ? $this->database->schema->getClassTable($class) : $class;
            if ($bucket->isCore()) {
                $driver = $this->database->getCoreDriver();
            } else {
                $storage = $this->database->getStorage($bucket->storage);
                if ($storage->isDedicated()) {
                    $table = explode('_', $table, 2)[1];
                }
                $driver = $storage->getDriver();
            }
            foreach ($callback($driver, $table) as $row) {
                $rows[] = $this->database->factory->getInstance($class, $row);
                if ($this->first) {
                    return array_pop($rows);
                }
            }
        }

        return $this->first ? null : $rows;
    }
}
