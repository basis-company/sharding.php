<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Attribute\Autoincrement;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Schema\Index;

class Change implements Indexing
{
    public const TABLE = 'sharding_change';

    public function __construct(
        #[Autoincrement]
        public int $id,
        public string $listener,
        public string $tablename,
        public string $action,
        public array $data,
        public array $context,
    ) {
    }

    public static function getIndexes(): array
    {
        return [
            new Index(["listener"]),
        ];
    }
}
