<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Schema\Index;

interface Indexing
{
    /**
     * @return Index[]
     */
    public static function getIndexes(): array;
}
