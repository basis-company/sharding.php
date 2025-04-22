<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Interface\Indexing;
use Basis\Sharded\Schema\UniqueIndex;

class Document implements Indexing
{
    public function __construct(
        public string $id,
        public string $name,
    ) {
    }
    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name']),
        ];
    }
}
