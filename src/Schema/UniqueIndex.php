<?php

namespace Basis\Sharded\Schema;

class UniqueIndex extends Index
{
    public function __construct(array $fields)
    {
        parent::__construct($fields, true);
    }
}
