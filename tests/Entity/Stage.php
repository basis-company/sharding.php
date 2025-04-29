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

    public static function getKey(array $data): ?string
    {
        return array_key_exists('year', $data) ? (string) $data['year'] : null;
    }
}
