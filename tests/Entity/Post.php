<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Locator;
use Basis\Sharding\Interface\Segment;

class Post implements Segment, Locator
{
    public function __construct(
        public int $id,
        public string $name,
    ) {
    }

    public static function castStorage(Database $database, Bucket $bucket): Storage
    {
        return $database->findOne(Storage::class, []);
    }

    public static function getSegment(): string
    {
        return 'posts';
    }
}
