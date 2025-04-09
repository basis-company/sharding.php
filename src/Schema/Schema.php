<?php

namespace Basis\Sharded\Schema;

use Basis\Sharded\Registry;

class Schema
{
    public function __construct(
        public readonly string $domain,
        public readonly array $models,
    ) {
    }
}
