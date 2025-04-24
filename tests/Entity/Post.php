<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Database;
use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Interface\Locator;
use Basis\Sharded\Interface\Segment;

class Post implements Segment, Locator
{
    public function __construct(
        public int $id,
        public string $name,
    ) {
    }

    public static function castStorage(Database $database, Bucket $bucket): Storage
    {
        return $database->findOrFail(Storage::class, ['id' => 1]);
    }

    public static function getSegment(): string
    {
        return 'posts';
    }
}
