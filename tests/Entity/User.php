<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Schema\UniqueIndex;

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
