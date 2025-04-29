<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Attribute\Sharding;
use Basis\Sharded\Interface\Domain;

#[Sharding]
class Activity implements Domain
{
    public function __construct(
        public string $id,
        public int $type,
    ) {
    }
    public static function getDomain(): string
    {
        return 'telemetry';
    }
}
