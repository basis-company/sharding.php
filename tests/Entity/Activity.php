<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Attribute\Sharding;
use Basis\Sharding\Interface\Domain;

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
