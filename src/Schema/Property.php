<?php

namespace Basis\Sharding\Schema;

class Property
{
    public function __construct(
        public readonly string $name,
        public readonly string $type,
        public bool $defaultAvailable = false,
        public $default = null,
    ) {
    }
}
