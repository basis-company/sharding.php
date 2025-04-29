<?php

namespace Basis\Sharding\Schema;

class UniqueIndex extends Index
{
    public function __construct(array $fields)
    {
        parent::__construct($fields, true);
    }
}
