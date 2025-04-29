<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Interface\Sharding;

class Stage implements Sharding
{
    public function __construct(
        public string $id,
        public int $year,
        public int $month,
    ) {
    }

    public static function getKey(array $data): ?string
    {
        return $data['year'] ?? null;
    }
}
