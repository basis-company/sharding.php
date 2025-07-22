<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Attribute\Sharding;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Schema\UniqueIndex;
use Basis\Sharding\Trait\References;

#[Sharding]
class User implements Indexing
{
    use References;

    public function __construct(
        public int $id,
        public string $name,
        public int $parent,
    ) {
    }

    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name'])
        ];
    }
}
