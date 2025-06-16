<?php

declare(strict_types=1);

namespace Basis\Sharding;

class Query
{
    public function __construct(
        public readonly Database $database,
        public readonly array $buckets
    ) {
    }
    public function using(string $query, array $params = [], bool $row = false): array
    {
        return (new Fetch($this->database, null, $row))->from($this->buckets)->query($query, $params);
    }

    public function row(string $query, array $params = []): array
    {
        return $this->using($query, $params, row: true);
    }

    public function rows(string $query, array $params = []): array
    {
        return $this->using($query, $params);
    }
}