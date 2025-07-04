<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Attribute\Tier;
use Basis\Sharding\Interface\Indexing;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Schema\Index;

#[Tier('cold')]
class Event implements Indexing, Segment
{
    public function __construct(
        public string $id,
        public string $type,
        public array $data,
    ) {
    }

    public static function getSegment(): string
    {
        return 'archive';
    }

    public static function getIndexes(): array
    {
        return [
            new Index(['type']),
        ];
    }
}
