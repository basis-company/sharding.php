<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Sharding;
use Basis\Sharding\Schema\Index;

class Stage implements Sharding, Indexing
{
    public function __construct(
        public string $id,
        public int $year,
        public int $month,
    ) {
    }

    public static function getKey(array $data): int|string|null
    {
        return array_key_exists('year', $data) ? $data['year'] : null;
    }

    public static function getIndexes(): array
    {
        return [
            new Index(['year']),
        ];
    }
}
