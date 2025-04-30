<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Interface\Sharding;

class Stage implements Sharding
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
}
