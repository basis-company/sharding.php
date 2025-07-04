<?php

namespace Basis\Sharding\Entity;

use Basis\Sharding\Interface\Domain;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Schema\UniqueIndex;

class Topology implements Domain, Indexing, Segment
{
    public const DRAFT_STATUS = 'draft';
    public const READY_STATUS = 'ready';
    public const STALE_STATUS = 'stale';

    public function __construct(
        public int $id,
        public string $name,
        public int $version,
        public string $status,
        public int $shards,
        public int $replicas,
        public int $tier,
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
        return 'core';
    }
}
