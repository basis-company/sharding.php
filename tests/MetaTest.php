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
        $this->assertSame($meta->getDomains(), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
        ]);
        $this->assertSame($meta->getTableDomain('sharded_bucket'), 'sharded_buckets');
        $this->assertSame($meta->getTableDomain('sharded_sequence'), 'sharded_sequences');
        $this->assertSame($meta->getTableDomain('sharded_storage'), 'sharded_storages');
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
        $this->assertSame($meta->getDomains(), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test'
        ]);
        $this->assertSame($meta->getClasses('test'), [User::class]);
        $this->assertSame($meta->getTable(User::class), 'test_user');
    }

    public function testSegments()
    {
        $meta = new Meta();
        $meta->register(Post::class);
        $this->assertSame($meta->getDomains(), [
            Bucket::BUCKET_BUCKET_NAME,
            Bucket::SEQUENCE_BUCKET_NAME,
            Bucket::STORAGE_BUCKET_NAME,
            'test_posts'
        ]);
        $this->assertSame($meta->getClasses('test_posts'), [Post::class]);
        $this->assertSame($meta->getTable(Post::class), 'test_post');
    }
}
