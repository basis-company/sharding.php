<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Interface\Segment;

class Post implements Segment
{
    public function __construct(
        public int $id,
        public string $name,
    ) {
    }

    public static function getSegment(): string
    {
        return 'posts';
    }
}
