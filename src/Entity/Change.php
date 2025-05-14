<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Schema\Index;

class Change implements Indexing
{
    public function __construct(
        public int $id,
        public string $listener,
        public string $table,
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

    public static function getSpaceName(): string
    {
        return "sharding_change";
    }
}
