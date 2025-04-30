<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Schema\Index;

class Subscription implements Indexing
{
    public function __construct(
        public int $id,
        public string $listener,
        public string $table,
    ) {
    }

    public static function getSpaceName(): string
    {
        return "sharding_subscription";
    }
    public static function getIndexes(): array
    {
        return [
            new Index(["table"]),
        ];
    }
}
