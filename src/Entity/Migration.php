<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Interface\Domain;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Schema\UniqueIndex;

class Migration implements Domain, Indexing, Segment
{
    public function __construct(
        public int $id,
        public string $name,
        public int $version,
        public int $bucket,
        public int $table,
        public string $key,
    ) {
    }

    public static function getDomain(): string
    {
        return 'sharding';
    }

    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name', 'version']),
        ];
    }

    public static function getSegment(): string
    {
        return 'migrations';
    }
}
