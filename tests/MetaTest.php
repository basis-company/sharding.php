<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Meta;
use Basis\Sharded\Test\Entity\Post;
use Basis\Sharded\Test\Entity\User;
use Exception;
use PHPUnit\Framework\TestCase;

class MetaTest extends TestCase
{
    public function testSystemBucket()
    {
        $meta = new Meta();
        $this->assertSame(array_keys($meta->segments), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
        ]);
        $this->assertSame($meta->getTableSegment('sharded_bucket'), 'sharded_buckets');
        $this->assertSame($meta->getTableSegment('sharded_sequence'), 'sharded_sequences');
        $this->assertSame($meta->getTableSegment('sharded_storage'), 'sharded_storages');
    }

    public function testDuplicateClass()
    {
        $meta = new Meta();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Class ' . User::class . ' already registered');
        $meta->register(User::class);
        $meta->register(User::class);
    }

    public function testDomainCasting()
    {
        $meta = new Meta();
        $meta->register(User::class);
        $this->assertSame(array_keys($meta->segments), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test'
        ]);
        $this->assertSame($meta->getSegmentByName('test')->getClasses(), [User::class]);
        $this->assertSame($meta->getClassTable(User::class), 'test_user');
    }

    public function testSegments()
    {
        $meta = new Meta();
        $meta->register(Post::class);
        $this->assertSame(array_keys($meta->segments), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test_posts'
        ]);
        $this->assertSame($meta->segments['test_posts']->getClasses(), [Post::class]);
        $this->assertSame($meta->getClassTable(Post::class), 'test_post');
    }
}
