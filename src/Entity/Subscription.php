<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Attribute\Autoincrement;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Schema\Index;

class Subscription implements Indexing
{
    public const TABLE = 'sharding_subscription';

    public function __construct(
        #[Autoincrement]
        public int $id,
        public string $listener,
        public string $tablename,
    ) {
    }

    public static function getIndexes(): array
    {
        return [
            new Index(["tablename"]),
        ];
    }
}
