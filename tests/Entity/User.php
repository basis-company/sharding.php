<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Interface\Indexing;
use Basis\Sharded\Schema\UniqueIndex;

class User implements Indexing
{
    public function __construct(
        public int $id,
        public string $name,
    ) {
    }

    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name'])
        ];
    }
}
