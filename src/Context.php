<?php

declare(strict_types=1);

namespace Basis\Sharded;

use Basis\Sharded\Interface\Driver;

class Context
{
    public function __construct(
        public Driver $driver,
        /**
         * @var \Basis\Shard\Entity\Bucket[]
         */
        public array $buckets,
        public string $table,
    ) {
    }
}
