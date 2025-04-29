<?php

namespace Basis\Sharded\Entity;

use Basis\Sharded\Interface\Domain;
use Basis\Sharded\Interface\Indexing;
use Basis\Sharded\Interface\Segment;
use Basis\Sharded\Schema\UniqueIndex;

class Topology implements Domain, Indexing, Segment
{
    public static function getDomain(): string
    {
        return 'sharded';
    }

    public static function getSegment(): string
    {
        return 'buckets';
    }

    public function __construct(
        public int $id,
        public string $name,
        public int $version,
        public int $shards = 1,
        public int $replicas = 1,
    ) {
    }

    public static function getIndexes(): array
    {
        return [
            new UniqueIndex(['name', 'version']),
        ];
    }
}
