<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Schema;
use Basis\Sharding\Test\Entity\Post;
use Basis\Sharding\Test\Entity\User;
use Exception;
use PHPUnit\Framework\TestCase;

class SchemaTest extends TestCase
{
    public function testSystemBucket()
    {
        $schema = new Schema();
        $this->assertSame(array_keys($schema->segments), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
        ]);
        $this->assertSame($schema->getTableSegment('sharding_bucket')->fullname, 'sharding_buckets');
        $this->assertSame($schema->getTableSegment('sharding_sequence')->fullname, 'sharding_sequences');
        $this->assertSame($schema->getTableSegment('sharding_storage')->fullname, 'sharding_storages');
    }

    public function testDuplicateClass()
    {
        $schema = new Schema();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Class ' . User::class . ' already registered');
        $schema->register(User::class);
        $schema->register(User::class);
    }

    public function testDomainCasting()
    {
        $schema = new Schema();
        $schema->register(User::class);
        $this->assertSame(array_keys($schema->segments), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test'
        ]);
        $this->assertSame($schema->getSegmentByName('test')->getClasses(), [User::class]);
        $this->assertSame($schema->getClassTable(User::class), 'test_user');
    }

    public function testSegments()
    {
        $schema = new Schema();
        $schema->register(Post::class);
        $this->assertSame(array_keys($schema->segments), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test_posts'
        ]);
        $this->assertSame($schema->segments['test_posts']->getClasses(), [Post::class]);
        $this->assertSame($schema->getClassTable(Post::class), 'test_post');
    }
}
