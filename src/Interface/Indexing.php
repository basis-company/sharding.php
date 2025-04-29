<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Schema\Index;

interface Indexing
{
    /**
     * @return Index[]
     */
    public static function getIndexes(): array;
}
