<?php

namespace Basis\Sharding\Schema;

class Index
{
    public readonly string $name;

    public function __construct(
        public readonly array $fields,
        public readonly bool $unique = false,
    ) {
        $this->name = implode("_", $fields);
    }
}
