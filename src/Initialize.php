<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Interface\Driver;

class Initialize
{
    public function __construct(
        public readonly Driver $driver,
        public array $classes,
    ) {
    }
}
