<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Locator;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Trait\ActiveRecord;

class Post implements Segment, Locator
{
    use ActiveRecord;

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
