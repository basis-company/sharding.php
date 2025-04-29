<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Attribute\Sharding;

#[Sharding]
class Activity
{
    public function __construct(
        public string $id,
        public int $type,
    ) {
    }
}
