<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Attribute\Reference;
use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Trait\ActiveRecord;
use Basis\Sharding\Trait\References;

class PostTag implements Segment
{
    use ActiveRecord;
    use References;

    public function __construct(
        public int $id,
        #[Reference(Post::class)]
        public int $post,
        public string $tag,
    ) {
    }

    public static function getSegment(): string
    {
        return 'posts';
    }
}
