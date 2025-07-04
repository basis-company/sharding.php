<?php

declare(strict_types=1);

namespace Basis\Sharding\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS)]
class Tier
{
    public function __construct(
        public string $name,
    ) {
    }
}
