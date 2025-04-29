<?php

namespace Basis\Sharding\Schema;

class Index
{
    public function __construct(
        public readonly array $fields,
        public readonly bool $unique = false,
    ) {
    }

    public function getName()
    {
        return implode("_", $this->fields) . ($this->unique ? "_unique" : "");
    }
}
