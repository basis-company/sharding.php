<?php

namespace Basis\Sharding\Test\Entity;

use Basis\Sharding\Attribute\Reference;
use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Interface\Locator;
use Basis\Sharding\Interface\Segment;
use Basis\Sharding\Trait\ActiveRecord;
use Basis\Sharding\Trait\References;

class Post implements Segment, Locator
{
    use ActiveRecord;
    use References;

    public function __construct(
        public int $id,
        public string $name,
        #[Reference(User::class)]
        public int $author,
        #[Reference('user')]
        public int $reviewer,
        #[Reference('test.user')]
        public int $administrator,
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
