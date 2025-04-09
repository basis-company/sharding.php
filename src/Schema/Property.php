<?php

namespace Basis\Sharded\Schema;

class Property
{
    public function __construct(
        public readonly string $name,
        public readonly string $type,
    ) {
    }
}
